"""Check Cortex Agents: names across agents and tools, agent-wide rules, spec size, and delegation.

The agent compiler in `app/compile/agents` runs these in its fixed order as it resolves each
agent; each check reads only the values it is given, so the order its diagnostics appear in
is the compiler's. The rules every published AGENT object shares -- its name, alias, tags,
model, profile, and deprecation -- read a `PublishedAgent`, so a CoWork profile, which publishes
as an AGENT too, is checked by the same code.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Protocol

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.agent import (
    RESERVED_AGENT_ALIASES,
    AgentModel,
    AgentProfile,
    ResolvedAgent,
    ResolvedAgentTool,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.lifecycle import is_eval_alias
from snowflake_semantic_tools.domain.model.setting import CONFIG_SOURCE
from snowflake_semantic_tools.domain.parse.names import RESERVED_WORDS
from snowflake_semantic_tools.domain.render.agent import render_agent_profile
from snowflake_semantic_tools.domain.sql import ident
from snowflake_semantic_tools.domain.validate.sql import (
    name_problem,
    qualified_name_problem,
    quoted_name_problem,
    stage_segment_problem,
)

SPEC_LIMIT_BYTES = 100_000
SPEC_WARNING_BYTES = 80_000
# The most bytes of profile JSON `ALTER AGENT ... SET PROFILE` accepts (docs/design/snowflake-behavior.md).
PROFILE_LIMIT_BYTES = 1_000


class PublishedAgent(Protocol):
    """What the rules every published AGENT object shares read: an agent's, or a CoWork profile's."""

    @property
    def name(self) -> str:
        """Return the name the object is keyed and staged by, as written."""
        ...

    @property
    def key(self) -> str:
        """Return the object's artifact key, which every diagnostic about it names as its subject."""
        ...

    @property
    def origin(self) -> Origin:
        """Return where the object is declared."""
        ...

    @property
    def alias(self) -> str | None:
        """Return the version alias the object sets, rendered for the target; None for none."""
        ...

    @property
    def tags(self) -> tuple[tuple[str, str], ...]:
        """Return the `(tag name, value)` pairs the object sets, in order."""
        ...

    @property
    def orchestration_model(self) -> str:
        """Return the orchestration model its settings resolve to; `auto` when none names one."""
        ...

    @property
    def profile(self) -> AgentProfile:
        """Return the display block `SET PROFILE` writes."""
        ...

    @property
    def deprecated(self) -> bool:
        """Return whether the object is retired from use."""
        ...


class AgentIdentities:
    """The names and display names claimed so far; a later object repeating one is reported.

    Attributes:
        names: By casefolded name, the last object to claim it; the delegation-cycle check walks
            an agent's.
    """

    def __init__(self, kind: str = "agent") -> None:
        """Track the names of one kind of AGENT object, which SST-VAL001 names as `kind`."""
        self.names: dict[str, PublishedAgent] = {}
        self._display_names: dict[str, PublishedAgent] = {}
        self._kind = kind

    def claim(self, model: PublishedAgent) -> tuple[Diagnostic, ...]:
        """Claim the object's name and display name, reporting each one an earlier object claimed.

        Diagnostics:
            SST-VAL001: the object's name repeats, ignoring case.
            SST-VAL549: the object's display name repeats, ignoring case.
        """
        found: list[Diagnostic] = []
        folded = model.name.casefold()
        if folded in self.names:
            found.append(D("SST-VAL001", type=self._kind, name=model.name, origin=model.origin))
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
    model: AgentModel,
    allowed_models: frozenset[str],
    tools: Sequence[ResolvedAgentTool],
    avatar_allowlist: frozenset[str] | None = None,
) -> list[Diagnostic]:
    """Check the rules about the agent as a whole: alias and tags, model, access, search, profile.

    Args:
        allowed_models: `snowflake.orchestration_models`.
        avatar_allowlist: `snowflake.profile.avatar_allowlist`; None when it is not configured.

    Diagnostics:
        SST-PRS025: the agent's alias is reserved: a version shortcut, or a Snowflake reserved
            word.
        SST-VAL561: the agent's alias starts with `EVAL_`, which eval-model versions reserve.
        SST-PRS005: the agent's name is written quoted or is not a segment a stage path can
            hold, its alias is not a plain identifier, or a tag name is not an identifier or a
            qualified name.
        SST-VAL543: the agent's orchestration model is not in `allowed_models`.
        SST-VAL545: the agent's `tool_not_accessible` is not accept, reject, or legacy.
        SST-VAL546: the agent enables analytical search without a cortex_search tool.
        SST-VAL548: the avatar is outside the allowlist, or the color is neither a plain
            color name nor a `var(--token)`.
        SST-VAL558: the profile renders more than `PROFILE_LIMIT_BYTES` of JSON.
        SST-VAL550: the agent is deprecated and an alias still points at a version of it.
    """
    diagnostics = [*_naming_problems(model), *_model_problems(model, allowed_models)]
    if model.tool_not_accessible not in (None, "accept", "reject", "legacy"):
        diagnostics.append(
            D("SST-VAL545", artifact=model.name, detail=f"is {model.tool_not_accessible!r}", subject=model.key)
        )
    if model.analytical_search and not any(tool.type == "cortex_search" for tool in tools):
        diagnostics.append(D("SST-VAL546", artifact=model.name, subject=model.key))
    diagnostics.extend(_display_problems(model, avatar_allowlist))
    return diagnostics


def published_agent_rules(
    model: PublishedAgent, allowed_models: frozenset[str], avatar_allowlist: frozenset[str] | None = None
) -> list[Diagnostic]:
    """Check the rules every published AGENT object shares, in the order `agent_rules` checks them.

    A CoWork profile is checked by these alone; an agent by `agent_rules`, which adds its own.

    Args:
        allowed_models: `snowflake.orchestration_models`.
        avatar_allowlist: `snowflake.profile.avatar_allowlist`; None when it is not configured.

    Diagnostics:
        SST-PRS025: the alias is reserved: a version shortcut, or a Snowflake reserved word.
        SST-VAL561: the alias starts with `EVAL_`, which eval-model versions reserve.
        SST-PRS005: the name is written quoted or is not a segment a stage path can hold, the
            alias is not a plain identifier, or a tag name is not an identifier or a qualified
            name.
        SST-VAL543: the orchestration model is not in `allowed_models`.
        SST-VAL548: the avatar is outside the allowlist, or the color is neither a plain
            color name nor a `var(--token)`.
        SST-VAL558: the profile renders more than `PROFILE_LIMIT_BYTES` of JSON.
        SST-VAL550: the object is deprecated and an alias still points at a version of it.
    """
    return [
        *_naming_problems(model),
        *_model_problems(model, allowed_models),
        *_display_problems(model, avatar_allowlist),
    ]


def _naming_problems(model: PublishedAgent) -> list[Diagnostic]:
    """Report a name no stage path can hold, then an unusable alias, then each unusable tag name."""
    diagnostics: list[Diagnostic] = []
    # The name keys the object and names the folder its specification is staged in.
    refused = quoted_name_problem(
        model.name, artifact=model.key, subject=model.key, origin=model.origin
    ) or stage_segment_problem(model.name, artifact=model.key, subject=model.key, origin=model.origin)
    if refused is not None:
        diagnostics.append(refused)
    if model.alias:
        diagnostics.extend(_alias_problems(model, model.alias))
    # Each tag name is written into ALTER AGENT unquoted when it can be.
    for name in (tag for tag, _ in model.tags):
        if not name:
            continue
        problem = name_problem(name, artifact=model.key, subject=model.key, origin=model.origin)
        if problem is not None and "." in name:
            problem = qualified_name_problem(name, artifact=model.key, subject=model.key, origin=model.origin)
        if problem is not None:
            diagnostics.append(problem)
    return diagnostics


def _model_problems(model: PublishedAgent, allowed_models: frozenset[str]) -> list[Diagnostic]:
    if model.orchestration_model in allowed_models:
        return []
    return [D("SST-VAL543", artifact=model.name, found=model.orchestration_model, subject=model.key)]


def _display_problems(model: PublishedAgent, avatar_allowlist: frozenset[str] | None) -> list[Diagnostic]:
    """Report the profile's problems, then an alias still set on a deprecated object."""
    diagnostics = _profile_problems(model, avatar_allowlist)
    if model.deprecated and model.alias:
        diagnostics.append(
            D(
                "SST-VAL550",
                artifact=model.name,
                detail=f"is deprecated and alias '{model.alias}' still points at a version of it",
                subject=model.key,
            )
        )
    return diagnostics


_COLOR_FORMS = re.compile(r"[A-Za-z]+|var\(--[A-Za-z0-9_-]+\)")


def reserved_alias(value: str) -> bool:
    """Report whether an alias, unquoted, is a version shortcut or a Snowflake reserved word."""
    return value.strip().upper() in RESERVED_AGENT_ALIASES | RESERVED_WORDS


def plain_alias(value: str) -> bool:
    """Report whether an alias renders unquoted, as written but upper-cased, and is not reserved.

    An alias is never quoted to rescue it: quoting makes it case-sensitive, so every later
    reference to it -- an eval's `agent_version:`, a rollback -- would have to match its case.
    So a name written quoted, one that is a valid identifier only once quoted, and a reserved
    word are not plain.
    """
    text = value.strip()
    try:
        rendered = str(ident(Identifier.parse(text)))
    except (TypeError, ValueError):
        return False
    return rendered == text.upper() and not reserved_alias(text)


def _alias_problems(model: PublishedAgent, alias: str) -> tuple[Diagnostic, ...]:
    """Report an alias that is reserved, else one with the eval-model prefix, else one not plain.

    Diagnostics:
        SST-PRS025: the alias is a version shortcut or a Snowflake reserved word.
        SST-VAL561: the alias starts with `EVAL_`, which eval-model versions reserve.
        SST-PRS005: the alias is not a plain identifier.
    """
    if reserved_alias(alias):
        return (D("SST-PRS025", artifact=model.name, value=alias, subject=model.key),)
    if is_eval_alias(alias):
        return (D("SST-VAL561", artifact=model.name, value=alias, subject=model.key),)
    if not plain_alias(alias):
        return (D("SST-PRS005", artifact=model.key, value=alias, subject=model.key, origin=model.origin),)
    return ()


def _profile_problems(model: PublishedAgent, avatar_allowlist: frozenset[str] | None) -> list[Diagnostic]:
    """Report an avatar outside the allowlist, a color in neither known form, then an oversize profile."""
    found: list[Diagnostic] = []
    avatar, color = model.profile.avatar, model.profile.color
    if avatar and avatar_allowlist is not None and avatar not in avatar_allowlist:
        found.append(
            D(
                "SST-VAL548",
                artifact=model.name,
                field="avatar",
                found=avatar,
                detail="is not in snowflake.profile.avatar_allowlist",
                subject=model.key,
            )
        )
    if color and not _COLOR_FORMS.fullmatch(color.strip()):
        found.append(
            D(
                "SST-VAL548",
                artifact=model.name,
                field="color",
                found=color,
                detail="is neither a plain color name nor a var(--token)",
                subject=model.key,
            )
        )
    size = len(render_agent_profile(model.profile).encode("utf-8"))
    if size > PROFILE_LIMIT_BYTES:
        found.append(D("SST-VAL558", artifact=model.name, size=size, subject=model.key, origin=model.origin))
    return found


def token_budget(agent: AgentModel) -> tuple[Diagnostic, ...]:
    """Report a token budget the agent sets for itself, which bounds orchestration and not spend.

    Only the agent's own `config.budget_tokens` counts, so a project-wide default from the
    `agents:` block or a folder route is not reported once per agent. A budget documented
    beside it as orchestration-only is not being read as a spend ceiling, and is not reported.

    Diagnostics:
        SST-VAL547: the agent sets `config.budget_tokens` and does not say it bounds orchestration.
    """
    own = agent.settings.get("budget_tokens")
    if own is None or own.source != CONFIG_SOURCE or agent.budget_tokens_documented:
        return ()
    return (D("SST-VAL547", artifact=agent.name, subject=agent.key),)


def spec_size(model: PublishedAgent, payload: str) -> tuple[Diagnostic, ...]:
    """Report a rendered spec at or over `SPEC_LIMIT_BYTES`, else one at or over `SPEC_WARNING_BYTES`.

    An agent's spec and a CoWork profile's are held to the same limits: both publish an AGENT.

    Diagnostics:
        SST-VAL511: the rendered spec is over the 100,000-byte limit.
        SST-VAL512: the rendered spec is over 80% of that limit.
    """
    size = len(payload.encode("utf-8"))
    if size >= SPEC_LIMIT_BYTES:
        return (D("SST-VAL511", artifact=model.name, size=size, subject=model.key),)
    if size >= SPEC_WARNING_BYTES:
        return (D("SST-VAL512", artifact=model.name, size=size, subject=model.key),)
    return ()


def delegations(resolved: ResolvedAgent) -> tuple[str, ...]:
    """Return the casefolded name of each agent whose tools the resolved agent's toolsets inherit."""
    return tuple(
        str(tool.resources.get("agent_name", "")).casefold() for tool in resolved.tools if tool.type == "agent_toolset"
    )


def delegation_cycle(graph: Mapping[str, tuple[str, ...]], agents: Mapping[str, PublishedAgent]) -> tuple[str, ...]:
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
