"""Check Cortex Agents: names across agents and tools, agent-wide rules, spec size, and delegation.

The agent compiler in `app/compile/agents` runs these in its fixed order as it resolves each
agent; each check reads only the values it is given, so the order its diagnostics appear in
is the compiler's.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import (
    RESERVED_AGENT_ALIASES,
    AgentModel,
    ResolvedAgent,
    ResolvedAgentTool,
)
from snowflake_semantic_tools.domain.validate.sql import name_problem, qualified_name_problem

SPEC_LIMIT_BYTES = 100_000
SPEC_WARNING_BYTES = 80_000


class AgentIdentities:
    """The agent names and display names claimed so far; a later agent repeating one is reported.

    Attributes:
        names: By casefolded name, the last agent to claim it; the delegation-cycle check walks these.
    """

    def __init__(self) -> None:
        self.names: dict[str, AgentModel] = {}
        self._display_names: dict[str, AgentModel] = {}

    def claim(self, model: AgentModel) -> tuple[Diagnostic, ...]:
        """Claim the agent's name and display name, reporting each one an earlier agent claimed.

        Diagnostics:
            SST-VAL001: the agent's name repeats, ignoring case.
            SST-VAL549: the agent's display name repeats, ignoring case.
        """
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


def tool_name_clashes(model: AgentModel, tools: Sequence[ResolvedAgentTool]) -> list[Diagnostic]:
    """Report each tool name seen before, then each earlier name it matches only ignoring case.

    Diagnostics:
        SST-VAL514: the agent declares one tool name twice.
        SST-VAL515: two of the agent's tool names differ only by case.
    """
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


def agent_rules(
    model: AgentModel, allowed_models: frozenset[str], tools: Sequence[ResolvedAgentTool]
) -> list[Diagnostic]:
    """Check the rules about the agent as a whole: alias and tags, model, access handling, search.

    Args:
        allowed_models: `snowflake.orchestration_models`.

    Diagnostics:
        SST-PRS025: the agent's alias is reserved.
        SST-PRS005: the agent's alias or a tag name is not an identifier or a qualified name.
        SST-VAL543: the agent's orchestration model is not in `allowed_models`.
        SST-VAL545: the agent's `tool_not_accessible` is not accept, reject, or legacy.
        SST-VAL546: the agent enables analytical search without a cortex_search tool.
    """
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
    if model.orchestration_model not in allowed_models:
        diagnostics.append(D("SST-VAL543", artifact=model.name, found=model.orchestration_model, subject=model.key))
    if model.tool_not_accessible not in (None, "accept", "reject", "legacy"):
        diagnostics.append(
            D("SST-VAL545", artifact=model.name, detail=f"is {model.tool_not_accessible!r}", subject=model.key)
        )
    if model.analytical_search and not any(tool.type == "cortex_search" for tool in tools):
        diagnostics.append(D("SST-VAL546", artifact=model.name, subject=model.key))
    return diagnostics


def spec_size(resolved: ResolvedAgent, payload: str) -> tuple[Diagnostic, ...]:
    """Report a rendered spec at or over `SPEC_LIMIT_BYTES`, else one at or over `SPEC_WARNING_BYTES`.

    Diagnostics:
        SST-VAL511: the rendered spec is over the 100,000-byte limit.
        SST-VAL512: the rendered spec is over 80% of that limit.
    """
    size = len(payload.encode("utf-8"))
    if size >= SPEC_LIMIT_BYTES:
        return (D("SST-VAL511", artifact=resolved.model.name, size=size, subject=resolved.model.key),)
    if size >= SPEC_WARNING_BYTES:
        return (D("SST-VAL512", artifact=resolved.model.name, size=size, subject=resolved.model.key),)
    return ()


def delegations(resolved: ResolvedAgent) -> tuple[str, ...]:
    """Return the casefolded identifier of each agent the resolved agent's agent tools delegate to."""
    return tuple(
        str(tool.resources.get("identifier", "")).casefold() for tool in resolved.tools if tool.type == "agent"
    )


def delegation_cycle(graph: Mapping[str, tuple[str, ...]], agents: Mapping[str, AgentModel]) -> tuple[str, ...]:
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


def unreferenced_extensions(
    agents: Sequence[ResolvedAgent], published: Iterable[tuple[str, str]], consumed: frozenset[str]
) -> tuple[Diagnostic, ...]:
    """Report each published extension that no agent pins and nothing else consumes, in `published` order.

    Args:
        published: Each extension this project publishes, as its artifact key and pinned alias.
        consumed: The artifact keys a plugin or profile already consumes.

    Diagnostics:
        SST-VAL804: a published skill or plugin is referenced by no agent and consumed by
            no plugin or profile.
    """
    referenced = {dependency for agent in agents for dependency in agent.skill_dependencies}
    return tuple(
        D("SST-VAL804", artifact=key, value=alias, subject=key)
        for key, alias in published
        if key not in referenced and key not in consumed
    )
