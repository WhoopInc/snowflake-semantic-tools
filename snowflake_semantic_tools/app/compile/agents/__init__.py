"""Resolve and validate authored Cortex Agents, then render complete specs.

`context` holds what compiling reads besides the agents, `resolve_tools` and
`resolve_skills` resolve an agent's `tools:` and `skills:` entries, and `compiled`
holds the compiled agent with the statements that publish it. This module runs the
compiler: agent by agent in authored order, then the checks that span every agent.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from hashlib import sha256

from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent, for_publication
from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext, ExtensionPin
from snowflake_semantic_tools.app.compile.agents.resolve_skills import resolve_skills
from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.app.compile.base import CompileResult, has_error
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import (
    RESERVED_AGENT_ALIASES,
    AgentModel,
    ResolvedAgent,
    ResolvedAgentTool,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.sql_checks import name_problem, qualified_name_problem
from snowflake_semantic_tools.domain.render.agent import desired_agent_definition, render_agent_json, render_agent_spec

__all__ = ["AgentCompileContext", "CompileAgents", "CompiledAgent", "ExtensionPin", "for_publication"]

_SPEC_LIMIT_BYTES = 100_000
_SPEC_WARNING_BYTES = 80_000


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
        tool, its tool name clashes and agent-wide rules, what `resolve_skills` reports, and
        its spec size. SST-REF022, then SST-VAL804, follow the last agent.

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
            SST-REF022: agents delegate to one another in a cycle.
            SST-VAL804: a published skill or plugin is referenced by no agent and consumed by
                no plugin or profile.
        """
        diagnostics: list[Diagnostic] = list(self._diagnostics)
        identities = _Identities()
        compiled: list[CompiledAgent] = []
        resolved_agents: list[ResolvedAgent] = []
        graph: dict[str, tuple[str, ...]] = {}
        for model in self._models:
            diagnostics.extend(identities.claim(model))
            resolved, payload, problems = self._resolve(model)
            resolved_agents.append(resolved)
            diagnostics.extend(problems)
            graph[model.name.casefold()] = _delegations(resolved)
            if not has_error(model.key, diagnostics):
                compiled.append(self._compile(model, resolved, payload))
        cycle = _cycle(graph, identities.names)
        if cycle:
            diagnostics.append(D("SST-REF022", cycle=" -> ".join(cycle)))
            compiled = []
        if self._models:
            diagnostics.extend(_unreferenced_extensions(resolved_agents, self._context))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))

    def _resolve(self, model: AgentModel) -> tuple[ResolvedAgent, str, tuple[Diagnostic, ...]]:
        """Resolve the agent with its inherited defaults, then render and size-check its spec."""
        resolved, problems = _resolve_agent(_inherit(model, self._context), self._context)
        payload = render_agent_json(resolved.model, resolved.tools)
        return resolved, payload, (*problems, *_spec_size(resolved, payload))

    def _compile(self, model: AgentModel, resolved: ResolvedAgent, payload: str) -> CompiledAgent:
        spec = render_agent_spec(resolved.model, resolved.tools)
        definition_fingerprint = sha256(desired_agent_definition(resolved.model, spec)).hexdigest()
        target = QualifiedName.from_parts(self._context.database, self._context.schema, model.name)
        return CompiledAgent(resolved, target, payload, definition_fingerprint)


class _Identities:
    """The agent names and display names claimed so far; a later agent repeating one is reported."""

    def __init__(self) -> None:
        # By casefolded name, the last agent to claim it; the cycle check walks these.
        self.names: dict[str, AgentModel] = {}
        self._display_names: dict[str, AgentModel] = {}

    def claim(self, model: AgentModel) -> tuple[Diagnostic, ...]:
        found: list[Diagnostic] = []
        folded = model.name.casefold()
        if folded in self.names:
            found.append(D("SST-VAL001", type="agent", name=model.name, origin=model.origin))
        self.names[folded] = model
        display_name = model.profile.display_name
        if display_name:
            other = self._display_names.get(display_name.casefold())
            if other is not None:
                found.append(
                    D("SST-VAL549", artifact=model.name, value=display_name, other=other.name, origin=model.origin)
                )
            self._display_names[display_name.casefold()] = model
        return tuple(found)


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
    diagnostics.extend(_tool_name_clashes(model, tools))
    diagnostics.extend(_agent_rules(model, context, tools))
    skills, dependencies, skill_diagnostics = resolve_skills(model, context)
    diagnostics.extend(skill_diagnostics)
    agent = ResolvedAgent(
        replace(model, skills=skills),
        tuple(tools),
        DiagnosticBag(diagnostics),
        skill_dependencies=dependencies,
    )
    return agent, tuple(diagnostics)


def _tool_name_clashes(model: AgentModel, tools: Sequence[ResolvedAgentTool]) -> list[Diagnostic]:
    """Report each tool name seen before, then each earlier name it matches only ignoring case."""
    diagnostics: list[Diagnostic] = []
    names: dict[str, ResolvedAgentTool] = {}
    for tool in tools:
        if tool.name in names:
            diagnostics.append(D("SST-VAL514", artifact=model.name, name=tool.name, subject=model.key))
        for other in names.values():
            if other.name.casefold() == tool.name.casefold() and other.name != tool.name:
                diagnostics.append(D("SST-VAL515", artifact=model.name, a=other.name, b=tool.name, subject=model.key))
        names[tool.name] = tool
    return diagnostics


def _agent_rules(
    model: AgentModel, context: AgentCompileContext, tools: Sequence[ResolvedAgentTool]
) -> list[Diagnostic]:
    """Check the rules about the agent as a whole: alias and tags, model, access handling, search."""
    diagnostics: list[Diagnostic] = []
    if model.alias and model.alias.upper() in RESERVED_AGENT_ALIASES:
        diagnostics.append(D("SST-PRS025", artifact=model.name, value=model.alias, subject=model.key))
    # The alias and each tag name are written into ALTER AGENT unquoted when they can be.
    for name in (model.alias, *(tag for tag, _ in model.tags)):
        if not name:
            continue
        problem = name_problem(name, artifact=model.key, subject=model.key, origin=model.origin)
        if problem is not None and "." in name:
            problem = qualified_name_problem(name, artifact=model.key, subject=model.key, origin=model.origin)
        if problem is not None:
            diagnostics.append(problem)
    if model.orchestration_model not in context.allowed_models:
        diagnostics.append(D("SST-VAL543", artifact=model.name, found=model.orchestration_model, subject=model.key))
    if model.tool_not_accessible not in (None, "accept", "reject", "legacy"):
        diagnostics.append(
            D("SST-VAL545", artifact=model.name, detail=f"is {model.tool_not_accessible!r}", subject=model.key)
        )
    if model.analytical_search and not any(tool.type == "cortex_search" for tool in tools):
        diagnostics.append(D("SST-VAL546", artifact=model.name, subject=model.key))
    return diagnostics


def _spec_size(resolved: ResolvedAgent, payload: str) -> tuple[Diagnostic, ...]:
    size = len(payload.encode("utf-8"))
    if size >= _SPEC_LIMIT_BYTES:
        return (D("SST-VAL511", artifact=resolved.model.name, size=size, subject=resolved.model.key),)
    if size >= _SPEC_WARNING_BYTES:
        return (D("SST-VAL512", artifact=resolved.model.name, size=size, subject=resolved.model.key),)
    return ()


def _delegations(resolved: ResolvedAgent) -> tuple[str, ...]:
    return tuple(
        str(tool.resources.get("identifier", "")).casefold() for tool in resolved.tools if tool.type == "agent"
    )


def _cycle(graph: Mapping[str, tuple[str, ...]], agents: Mapping[str, AgentModel]) -> tuple[str, ...]:
    """Return the first delegation cycle among this project's agents, or an empty tuple.

    A delegation target is matched to an agent by its casefolded name, whole or as the last
    part of a qualified name; a target outside the project closes no cycle. Agents are
    walked in name order, so the same cycle is reported on every run.
    """
    names_by_target = {f"{name}".casefold(): name for name in agents}
    normalized: dict[str, tuple[str, ...]] = {}
    for name, targets in graph.items():
        normalized[name] = tuple(
            other
            for target in targets
            for other in names_by_target
            if target.endswith(f".{other}".casefold()) or target == other
        )
    visiting: list[str] = []
    complete: set[str] = set()

    def walk(name: str) -> tuple[str, ...]:
        if name in visiting:
            index = visiting.index(name)
            return tuple(visiting[index:] + [name])
        if name in complete:
            return ()
        visiting.append(name)
        for target in normalized.get(name, ()):
            cycle = walk(target)
            if cycle:
                return cycle
        visiting.pop()
        complete.add(name)
        return ()

    for name in sorted(normalized):
        cycle = walk(name)
        if cycle:
            return cycle
    return ()


def _unreferenced_extensions(agents: Sequence[ResolvedAgent], context: AgentCompileContext) -> tuple[Diagnostic, ...]:
    """Report each published skill, then plugin, that no agent pins and nothing else consumes."""
    referenced = {dependency for agent in agents for dependency in agent.skill_dependencies}
    return tuple(
        D("SST-VAL804", artifact=pin.key, value=pin.alias, subject=pin.key)
        for pin in (*context.skills.values(), *context.plugins.values())
        if pin.key not in referenced and pin.key not in context.consumed
    )
