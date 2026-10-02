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
from snowflake_semantic_tools.domain.render.agent import desired_agent_definition, render_agent_json, render_agent_spec
from snowflake_semantic_tools.domain.validate.agent import (
    AgentIdentities,
    agent_rules,
    delegation_cycle,
    delegations,
    spec_size,
    token_budget,
    tool_name_clashes,
    unreferenced_extensions,
)
from snowflake_semantic_tools.domain.validate.agent_spec import (
    instruction_problems,
    near_duplicate_descriptions,
    resource_keys,
    spec_completeness,
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

        Each agent reports, in order: its name clashes, its own token budget, what
        `resolve_tool` reports for each tool, its tool name clashes and near-duplicate
        descriptions, its agent-wide rules and instructions, what `resolve_skills` reports,
        and its rendered spec's completeness, resource keys, and size. SST-REF022, then
        SST-VAL804, follow the last agent.

        Diagnostics:
            SST-VAL001: an agent name repeats, ignoring case.
            SST-VAL549: an agent's display name repeats, ignoring case.
            SST-VAL547: an agent sets its own token budget, which bounds orchestration only.
            SST-VAL514: an agent declares one tool name twice.
            SST-VAL515: two of an agent's tool names differ only by case.
            SST-VAL519: two of an agent's tools have near-identical descriptions.
            SST-PRS025: an agent's alias is reserved.
            SST-PRS005: an agent's alias or a tag name is not an identifier or a qualified name.
            SST-VAL543: an agent's orchestration model is not in `snowflake.orchestration_models`.
            SST-VAL545: an agent's `tool_not_accessible` is not accept, reject, or legacy.
            SST-VAL546: an agent enables analytical search without a cortex_search tool.
            SST-VAL548: an agent's avatar or color is outside its allowlist or known forms.
            SST-VAL550: a deprecated agent still has an alias.
            SST-VAL535: an agent's instructions name a tool it does not have.
            SST-VAL536: an agent's instructions route a topic to a tool that excludes it.
            SST-VAL510: a rendered spec lacks a section the agent carries.
            SST-VAL529: a rendered spec gives a built-in tool resources.
            SST-VAL530: a rendered spec's resource key names no rendered tool.
            SST-VAL511: a rendered spec is over the 100,000-byte limit.
            SST-VAL512: a rendered spec is over 80% of that limit.
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
        """Resolve the agent with its inherited defaults, then render its spec and check it whole.

        The authored token budget is read before the defaults fill it; the rendered spec is
        checked for completeness, its resource keys, and its size, in that order.
        """
        resolved, problems = _resolve_agent(_inherit(model, self._context), self._context)
        document = render_agent_spec(resolved.model, resolved.tools)
        payload = render_agent_json(resolved.model, resolved.tools)
        incomplete = spec_completeness(resolved.model, resolved.tools, document)
        return (
            resolved,
            payload,
            (
                *token_budget(model),
                *problems,
                *((incomplete,) if incomplete is not None else ()),
                *resource_keys(resolved.model, document),
                *spec_size(resolved, payload),
            ),
        )

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
    """Resolve each tool, check the tools together and the agent-wide rules, then resolve the skills.

    The diagnostics come in that order -- tool names, descriptions, agent-wide rules, then
    instructions against the tools -- and the resolved agent carries them too.
    """
    diagnostics: list[Diagnostic] = []
    tools: list[ResolvedAgentTool] = []
    for authored in model.tools:
        resolved, problems = resolve_tool(model, authored, context)
        diagnostics.extend(problems)
        if resolved is not None:
            tools.append(resolved)
    diagnostics.extend(tool_name_clashes(model, tools))
    diagnostics.extend(near_duplicate_descriptions(model, tools))
    diagnostics.extend(agent_rules(model, context.allowed_models, tools, context.avatar_allowlist))
    diagnostics.extend(instruction_problems(model, tools))
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
