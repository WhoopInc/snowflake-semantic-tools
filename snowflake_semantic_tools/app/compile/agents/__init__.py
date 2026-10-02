"""Resolve and validate authored Cortex Agents, then render complete specs.

`context` holds what compiling reads besides the agents, `resolve_tools` and
`resolve_skills` resolve an agent's `tools:` and `skills:` entries, and `compiled`
holds the compiled agent with the statements that publish it. This module runs the
compiler: agent by agent in authored order, then the checks that span every agent. The
checks themselves are pure and live in `domain.validate.agent`; this module only orders
them and decides which agents compile.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from hashlib import sha256

from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent, for_publication
from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext, ExtensionPin
from snowflake_semantic_tools.app.compile.agents.resolve_skills import resolve_skills
from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.app.compile.base import CompileResult, has_error
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import (
    AgentModel,
    ResolvedAgent,
    ResolvedAgentTool,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.agent import (
    agent_render_checks,
    desired_agent_definition,
    render_agent_json,
    render_agent_spec,
)
from snowflake_semantic_tools.domain.validate.agent import (
    AgentIdentities,
    agent_rules,
    delegation_cycle,
    delegations,
    spec_size,
    tool_name_clashes,
    unreferenced_extensions,
)

__all__ = ["AgentCompileContext", "CompileAgents", "CompiledAgent", "ExtensionPin", "for_publication"]


class CompileAgents:
    """Compile each agent: inherit the `agents:` defaults, resolve it, check it, render its spec.

    `diagnostics` are the loader's; they come first, and an error among them that names an
    agent keeps that agent back like one found here.
    """

    def __init__(
        self, models: tuple[AgentModel, ...], diagnostics: DiagnosticBag, context: AgentCompileContext
    ) -> None:
        self._models = models
        self._diagnostics = diagnostics
        self._context = context

    def run_result(self) -> CompileResult:
        """Compile the agents in authored order, then check what spans all of them.

        An agent is compiled unless an error names it, including one reported for an
        earlier agent with the same key. A delegation cycle drops every agent. The
        project's extensions are checked for references only when it has agents.

        Each agent reports, in order: its name clashes, what `resolve_tool` reports for each
        tool, its tool name clashes and agent-wide rules, what `resolve_skills` reports, its
        spec size, and what `agent_render_checks` reports. SST-REF022, then SST-VAL804, follow
        the last agent.

        Diagnostics:
            SST-VAL001: an agent name repeats, ignoring case.
            SST-VAL549: an agent's display name repeats, ignoring case.
            SST-VAL514: an agent declares one tool name twice.
            SST-VAL515: two of an agent's tool names differ only by case.
            SST-PRS025: an agent's alias is reserved.
            SST-PRS005: an agent's alias or a tag name is not an identifier or a qualified name.
            SST-VAL543: an agent's orchestration model is not in `snowflake.orchestration_models`.
            SST-VAL545: an agent's `tool_not_accessible` is not accept, reject, or legacy.
            SST-VAL546: an agent enables analytical search without a cortex_search tool.
            SST-VAL511: a rendered spec is over the 100,000-byte limit.
            SST-VAL512: a rendered spec is over 80% of that limit.
            SST-RND010, SST-RND011, SST-RND013: as `agent_render_checks` reports them.
            SST-REF022: agents delegate to one another in a cycle.
            SST-VAL804: a published skill or plugin is referenced by no agent and consumed by
                no plugin or profile.
        """
        diagnostics: list[Diagnostic] = list(self._diagnostics)
        identities = AgentIdentities()
        compiled: list[CompiledAgent] = []
        resolved_agents: list[ResolvedAgent] = []
        graph: dict[str, tuple[str, ...]] = {}
        for model in self._models:
            diagnostics.extend(identities.claim(model))
            resolved, payload, problems = self._resolve(model)
            resolved_agents.append(resolved)
            diagnostics.extend(problems)
            graph[model.name.casefold()] = delegations(resolved)
            if not has_error(model.key, diagnostics):
                compiled.append(self._compile(model, resolved, payload))
        cycle = delegation_cycle(graph, identities.names)
        if cycle:
            diagnostics.append(D("SST-REF022", cycle=" -> ".join(cycle)))
            compiled = []
        if self._models:
            diagnostics.extend(_unreferenced_extensions(resolved_agents, self._context))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))

    def _resolve(self, model: AgentModel) -> tuple[ResolvedAgent, str, tuple[Diagnostic, ...]]:
        """Resolve the agent with its inherited defaults, then render, size-check, and check its spec."""
        resolved, problems = _resolve_agent(_inherit(model, self._context), self._context)
        payload = render_agent_json(resolved.model, resolved.tools)
        checks = agent_render_checks(resolved.model, resolved.tools, payload)
        return resolved, payload, (*problems, *spec_size(resolved, payload), *checks)

    def _compile(self, model: AgentModel, resolved: ResolvedAgent, payload: str) -> CompiledAgent:
        spec = render_agent_spec(resolved.model, resolved.tools)
        definition_fingerprint = sha256(desired_agent_definition(resolved.model, spec)).hexdigest()
        target = QualifiedName.from_parts(self._context.database, self._context.schema, model.name)
        return CompiledAgent(resolved, target, payload, definition_fingerprint)


def _inherit(model: AgentModel, context: AgentCompileContext) -> AgentModel:
    """Fill what the agent leaves unset from the `agents:` defaults; a model of `auto` counts as unset."""
    return replace(
        model,
        orchestration_model=(
            model.orchestration_model if model.orchestration_model != "auto" else context.orchestration_model
        ),
        budget_seconds=model.budget_seconds or context.budget_seconds,
        budget_tokens=model.budget_tokens or context.budget_tokens,
        tool_not_accessible=model.tool_not_accessible or context.tool_not_accessible,
        analytical_search=(
            model.analytical_search if model.analytical_search is not None else context.analytical_search
        ),
        alias=model.alias or context.alias,
    )


def _resolve_agent(model: AgentModel, context: AgentCompileContext) -> tuple[ResolvedAgent, tuple[Diagnostic, ...]]:
    """Resolve each tool, check the tool names and the agent-wide rules, then resolve the skills.

    The diagnostics come in that order, and the resolved agent carries them too.
    """
    diagnostics: list[Diagnostic] = []
    tools: list[ResolvedAgentTool] = []
    for authored in model.tools:
        resolved, problems = resolve_tool(model, authored, context)
        diagnostics.extend(problems)
        if resolved is not None:
            tools.append(resolved)
    diagnostics.extend(tool_name_clashes(model, tools))
    diagnostics.extend(agent_rules(model, context.allowed_models, tools))
    skills, dependencies, skill_diagnostics = resolve_skills(model, context)
    diagnostics.extend(skill_diagnostics)
    agent = ResolvedAgent(
        replace(model, skills=skills),
        tuple(tools),
        DiagnosticBag(diagnostics),
        skill_dependencies=dependencies,
    )
    return agent, tuple(diagnostics)


def _unreferenced_extensions(agents: Sequence[ResolvedAgent], context: AgentCompileContext) -> tuple[Diagnostic, ...]:
    """Report each published skill, then plugin, that no agent pins and nothing else consumes."""
    published = ((pin.key, pin.alias) for pin in (*context.skills.values(), *context.plugins.values()))
    return unreferenced_extensions(agents, published, context.consumed)
