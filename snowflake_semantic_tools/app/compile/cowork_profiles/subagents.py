"""Resolve a CoWork profile's subagents: the agents its router may hand a question to.

CoWork profiles are a Snowflake private preview (PrPr) feature; a subagent resolves to the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A subagent's `agent:` is one call. `agent('<name>')` names an enabled agent of this project, as
an agent toolset's `agent()` does, and takes that agent's display name, comment, and sample
questions for whatever the entry leaves unset; the agent publishes first. `tool('<name>')` names
an `agent` tool member, as a toolset's `identifier:` does, and routes to the agent its relation
names, which SST does not publish, so it takes no defaults. Its diagnostics are reported as each
entry resolves, so they stay with resolution here rather than in `domain.validate`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel, CoworkSubagent, ResolvedSubagent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolKind


@dataclass(frozen=True, slots=True)
class CoworkCompileContext:
    """What compiling CoWork profiles resolves against: the agents' context, and the agents.

    Attributes:
        agent: The context the agents compiled against, with the `agents:` block root's
            environment. Its `agents` holds where each enabled agent publishes, by casefolded
            name; its tool catalog resolves a `tool()` subagent; and its skills, plugins,
            extensions, allowed models, and avatar allowlist check a profile as an agent's.
        agents: Every enabled agent, by casefolded name; the first of two with one name keeps it.
        targets: Where each enabled profile publishes, by casefolded name, as its
            `cowork_profiles:` settings place it; the first of two with one name keeps it.
        database, schema: Where a profile `targets` does not name publishes: the target's own.
    """

    agent: AgentCompileContext
    agents: Mapping[str, AgentModel]
    targets: Mapping[str, QualifiedName]
    database: str
    schema: str

    def target(self, model: CoworkProfileModel) -> QualifiedName:
        """Return where the profile publishes: its entry in `targets`, else the default location.

        Raises:
            ValueError: a part of the name is not a valid identifier.
        """
        return self.targets.get(model.name.casefold()) or QualifiedName.from_parts(
            self.database, self.schema, model.name
        )


def resolve_subagents(
    model: CoworkProfileModel, context: CoworkCompileContext
) -> tuple[tuple[ResolvedSubagent, ...], tuple[Diagnostic, ...]]:
    """Resolve each subagent in authored order, then check it is described and routed to once.

    A subagent that does not resolve is left out. Each that resolves is checked for its display
    name and description, then against the agents the earlier ones resolved to: an entry
    repeating an earlier call loading already reported, so only a different call reaching the
    same agent is reported here; either way the repeat is left out.

    Returns:
        The subagents that resolved, in authored order, and every diagnostic in the order found.

    Diagnostics:
        SST-REF012: an `agent()` subagent names no enabled agent of this project.
        SST-VAL875: an `agent()` subagent names a deprecated agent; it still resolves.
        SST-REF010: a `tool()` subagent names no tool member.
        SST-VAL872: a `tool()` subagent's member is not an `agent` member.
        SST-REF018, SST-REF019: a `tool()` subagent's member has no usable relation for the target.
        SST-VAL874: a subagent has no display name, or no description, after its defaults.
        SST-VAL873: a subagent resolves to the agent an earlier one resolved to, through another call.
    """
    found: list[ResolvedSubagent] = []
    diagnostics: list[Diagnostic] = []
    claimed: dict[tuple[str, str, str], tuple[str, str]] = {}
    for index, subagent in enumerate(model.subagents):
        if subagent.reference == "agent":
            resolved = _project_agent(model, index, subagent, context, diagnostics)
        else:
            resolved = _agent_member(model, index, subagent, context.agent, diagnostics)
        if resolved is None:
            continue
        diagnostics.extend(_undescribed(model, index, subagent, resolved))
        identity = (subagent.reference, subagent.name.casefold())
        earlier = claimed.get(resolved.fqn.folded)
        if earlier is None:
            claimed[resolved.fqn.folded] = identity
            found.append(resolved)
        elif earlier != identity:
            diagnostics.append(
                D(
                    "SST-VAL873",
                    origin=subagent.origin,
                    subject=model.key,
                    artifact=model.name,
                    index=index,
                    function=subagent.reference,
                    name=subagent.name,
                )
            )
    return tuple(found), tuple(diagnostics)


def _project_agent(
    model: CoworkProfileModel,
    index: int,
    subagent: CoworkSubagent,
    context: CoworkCompileContext,
    diagnostics: list[Diagnostic],
) -> ResolvedSubagent | None:
    """An `agent()` subagent: an enabled agent of this project, whose own values fill what is unset."""
    folded = subagent.name.casefold()
    target = context.agent.agents.get(folded)
    agent = context.agents.get(folded)
    if target is None or agent is None:
        # A disabled agent is no enabled agent, as an agent toolset's `agent()` reports it.
        diagnostics.append(D("SST-REF012", name=subagent.name, subject=model.key, origin=subagent.origin))
        return None
    if agent.deprecated:
        diagnostics.append(
            D(
                "SST-VAL875",
                origin=subagent.origin,
                subject=model.key,
                artifact=model.name,
                index=index,
                name=subagent.name,
            )
        )
    return ResolvedSubagent(
        target,
        subagent.display_name if subagent.display_name is not None else agent.profile.display_name or agent.name,
        subagent.description if subagent.description is not None else _described_by(agent),
        subagent.sample_questions if subagent.sample_questions is not None else agent.sample_questions,
        depends_on=artifact_key("agent", folded),
    )


def _agent_member(
    model: CoworkProfileModel,
    index: int,
    subagent: CoworkSubagent,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> ResolvedSubagent | None:
    """A `tool()` subagent: the agent an `agent` tool member's relation names, with no defaults.

    A member that resolves to no relation is left out; for a defined member, which has none,
    nothing reports it, as for an agent toolset over one.
    """
    backing, problems = context.tools.resolve(subagent.name)
    # The catalog names no subject for a member it cannot find; the profile is what it keeps back.
    diagnostics.extend(_against(model, subagent, problems))
    if backing is None:
        return None
    if backing.type != ToolKind.AGENT.value:
        diagnostics.append(
            D(
                "SST-VAL872",
                origin=subagent.origin,
                subject=model.key,
                artifact=model.name,
                index=index,
                name=subagent.name,
                found=backing.type,
            )
        )
        return None
    relation, relation_problems = context.tools.relation(backing)
    diagnostics.extend(relation_problems)
    if relation is None:
        return None
    return ResolvedSubagent(
        relation,
        subagent.display_name or "",
        subagent.description or "",
        subagent.sample_questions or (),
        external=True,
        member=backing.name,
    )


def _described_by(agent: AgentModel) -> str:
    """The agent's own comment, without the ownership marker publishing adds; blank describes nothing."""
    comment = agent.comment or ""
    return comment if comment.strip() else ""


def _undescribed(
    model: CoworkProfileModel, index: int, subagent: CoworkSubagent, resolved: ResolvedSubagent
) -> tuple[Diagnostic, ...]:
    return tuple(
        D(
            "SST-VAL874",
            origin=subagent.origin,
            subject=model.key,
            artifact=model.name,
            index=index,
            function=subagent.reference,
            name=subagent.name,
            field=field,
        )
        for field, value in (("display_name", resolved.display_name), ("description", resolved.description))
        if not value.strip()
    )


def _against(model: CoworkProfileModel, subagent: CoworkSubagent, problems: Iterable[Diagnostic]) -> list[Diagnostic]:
    """Name the profile as the subject of each problem that names none, at the subagent."""
    return [
        item if item.subject is not None else replace(item, subject=model.key, origin=item.origin or subagent.origin)
        for item in problems
    ]
