"""Read what each planned update replaces, and compare it member by member with what SST renders.

Plan decides an update only while the live object still matches what SST last wrote, so the live
definition is exactly what apply replaces. For each update of a type it can compare, this reads
that definition, builds the one SST compiled, and records their `member_diff` on the change. It
reads nothing for any other change, and a read Snowflake refuses only leaves the change without
its member diff (SST-PLN041): the plan is otherwise the same.

Each artifact type has a desired builder, from what compiled, and a live read, from a session and
a `LiveObject` saying where the published object is, so `sst diff` compares the same definitions
plan does, from what a target's state records rather than from a planned change.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompiledView
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.coco_profiles import CompiledCocoProfile
from snowflake_semantic_tools.app.compile.cowork_profiles import CompiledCoworkProfile
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.plan_diff_composites import (
    compiled_as,
    desired_eval,
    desired_extension,
    live_eval,
    live_extension,
)
from snowflake_semantic_tools.app.prior import served_version
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change
from snowflake_semantic_tools.domain.model.tool import ToolKind
from snowflake_semantic_tools.domain.plan.definitions.agent import (
    COCO_PROPERTIES_KIND,
    COWORK_PROPERTIES_KIND,
    PROPERTIES_KEYS,
    PROPERTIES_KIND,
    agent_properties,
    spec_definition,
    with_properties,
)
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import desired_view, live_view, live_view_from_yaml
from snowflake_semantic_tools.domain.plan.definitions.tool import (
    desired_routine,
    desired_search,
    live_routine,
    live_search,
    search_query,
)
from snowflake_semantic_tools.domain.plan.member_diff import Definition, diff_definitions
from snowflake_semantic_tools.domain.plan.member_fingerprints import changed_members, member_fingerprints
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.agent import render_agent_profile
from snowflake_semantic_tools.domain.state import AppliedEntry


@dataclass(frozen=True, slots=True)
class LiveObject:
    """Where an artifact's published object is, and what is known of it, for its live read.

    Attributes:
        name: The object: what plan observed under the key, else what state records.
        alias: An agent's desired version alias, which picks the version it serves.
        comment: The comment SHOW lists on it, which a routine's DESCRIBE does not report.
        routine_signature: A procedure's or function's argument types, as observed.
        recorded: What state records of the artifact, from which an eval's earlier publish is read,
            and the version of a skill or plugin.
        config_folder: Where an eval's configs are staged: `@<stage>/<agent segment>`.
    """

    name: QualifiedName
    alias: str | None = None
    comment: str | None = None
    routine_signature: tuple[str, ...] = ()
    recorded: AppliedEntry | None = None
    config_folder: str | None = None


@dataclass(frozen=True, slots=True)
class Comparison:
    """How one artifact type is compared: its desired definition, and the live one a session reads.

    Each returns None for an artifact of the type that is not compared, such as a stage tool.
    """

    desired: Callable[[CompiledArtifact], Definition | None]
    live: Callable[[SnowflakePort, CompiledArtifact, LiveObject], Definition]


def _live_agent(port: SnowflakePort, compiled: CompiledArtifact, live: LiveObject) -> Definition:
    """The served version's specification, with the agent's properties as SHOW AGENTS lists them.

    A property SHOW AGENTS does not list, such as `is_secure` on a role that cannot see it, is
    left out, so it is not compared. A CoWork profile is an AGENT too, read the same way, its
    properties keyed by its own type.
    """
    versions = port.observe_agent_versions(live.name)
    served = served_version(versions.versions, live.alias)
    if served is None or served.specification is None:
        raise SnowflakePortError("the role may not read the specification of the version the agent serves")
    try:
        specification = spec_definition(served.specification)
    except ValueError as exc:
        raise SnowflakePortError(f"the specification the agent serves is not a JSON object: {exc}") from exc
    row = port.show_row("AGENT", live.name) or {}
    secure = row.get("is_secure")
    shown = agent_properties(
        comment=row.get("comment"),
        profile=row.get("profile"),
        aliases=versions.aliases,
        secure=secure.strip().casefold() in ("true", "y", "yes") if secure is not None else None,
        kind=compiled.artifact_type,
    )
    return with_properties(specification, shown)


def _desired_agent(compiled: CompiledArtifact) -> Definition:
    """The specification SST renders, with the agent's comment, profile, alias and SECURE as it sets them."""
    agent = compiled_as(compiled, CompiledAgent)
    model = agent.resolved.model
    return _desired_agent_object(
        agent.payload, model.comment, model.profile, model.alias, model.secure is True, PROPERTIES_KIND
    )


def _desired_cowork_profile(compiled: CompiledArtifact) -> Definition:
    """The specification SST renders, with the profile's comment, display, alias, and SECURE false."""
    profile = compiled_as(compiled, CompiledCoworkProfile)
    model = profile.resolved.model
    # A profile sets no SECURE of its own, so it publishes `SECURE = FALSE`.
    return _desired_agent_object(
        profile.payload, model.comment, model.display, model.alias, False, COWORK_PROPERTIES_KIND
    )


def _desired_coco_profile(compiled: CompiledArtifact) -> Definition:
    """The specification SST renders, with the profile's comment, no display or alias, and SECURE false."""
    profile = compiled_as(compiled, CompiledCocoProfile)
    return _desired_agent_object(
        profile.payload, profile.resolved.model.comment, AgentProfile(), None, False, COCO_PROPERTIES_KIND
    )


def _desired_agent_object(
    payload: str, comment: str | None, profile: AgentProfile, alias: str | None, secure: bool, kind: str
) -> Definition:
    """An AGENT object's rendered specification with its properties, keyed by its `kind`."""
    return with_properties(
        spec_definition(payload),
        agent_properties(
            comment=comment or "",
            profile=render_agent_profile(profile),
            aliases=(alias,) if alias else (),
            secure=secure,
            kind=kind,
        ),
    )


def _desired_tool(compiled: CompiledArtifact) -> Definition | None:
    """A procedure, function or search service as SST renders it; None for a stage."""
    tool = compiled_as(compiled, CompiledTool)
    member = tool.member
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        return desired_search(member, search_query(tool.rendered.ddl))
    if member.type in (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value):
        return desired_routine(member, procedure=member.type == ToolKind.PROCEDURE.value)
    return None


def _live_tool(port: SnowflakePort, compiled: CompiledArtifact, live: LiveObject) -> Definition:
    """A procedure, function or search service as DESCRIBE shows it."""
    member = compiled_as(compiled, CompiledTool).member
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        described = port.describe_properties("CORTEX SEARCH SERVICE", live.name)
        if described is None:
            raise SnowflakePortError(f"no search service {live.name.sql} to describe")
        return live_search(member.name, described)
    signature = live.routine_signature or tuple(parameter.type for parameter in member.signature)
    described_routine = port.describe_routine(member.type.upper(), live.name, signature)
    return live_routine(member.name, described_routine, live.comment, procedure=member.type == ToolKind.PROCEDURE.value)


def _live_view(port: SnowflakePort, _compiled: CompiledArtifact, live: LiveObject) -> Definition:
    """The view as `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` reads it, else as DESCRIBE does.

    The YAML read reports every member and property the desired side states; DESCRIBE, read only
    where the function cannot be called, leaves out what it does not report.

    Raises:
        SnowflakePortError: a read was refused, or the YAML read returned no semantic view.
    """
    document = port.read_semantic_view_yaml(live.name)
    if document is None:
        return live_view(port.describe_semantic_view(live.name))
    try:
        return live_view_from_yaml(document)
    except ValueError as exc:
        raise SnowflakePortError(f"the view's YAML could not be read as a semantic view: {exc}") from exc


COMPARISONS: Mapping[str, Comparison] = {
    "semantic_view": Comparison(lambda compiled: desired_view(compiled_as(compiled, CompiledView).view), _live_view),
    "agent": Comparison(_desired_agent, _live_agent),
    # Snowflake private preview (PrPr): CoWork profiles
    "cowork_profile": Comparison(_desired_cowork_profile, _live_agent),
    # Snowflake private preview (PrPr): Cortex Code agent profiles
    "coco_profile": Comparison(_desired_coco_profile, _live_agent),
    "tool": Comparison(_desired_tool, _live_tool),
    "skill": Comparison(
        desired_extension, lambda port, _compiled, live: live_extension(port, live.name, live.recorded)
    ),
    "plugin": Comparison(
        desired_extension, lambda port, _compiled, live: live_extension(port, live.name, live.recorded)
    ),
    "eval": Comparison(desired_eval, lambda port, _compiled, live: live_eval(port, live.config_folder, live.recorded)),
}


def diff_updates(
    changes: Sequence[Change],
    compiled: Mapping[str, CompiledArtifact],
    readers: Fanout[SnowflakePort],
    recorded: Mapping[str, AppliedEntry] = MappingProxyType({}),
    comparisons: Mapping[str, Comparison] = COMPARISONS,
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Return the changes with each comparable update's member diff, and a diagnostic per refused read.

    An update is comparable when `comparisons` has its artifact type and `compiled` its artifact.
    The reads run on `readers`, one item each, and the changes come back in their order;
    `recorded` is what state records of each artifact, which an eval's earlier publish is read from.

    Diagnostics:
        SST-PLN041: reading an update's live definition failed; the change has no member diff.
    """
    comparable = tuple(
        index
        for index, change in enumerate(changes)
        if change.action is Action.UPDATE and change.artifact_type in comparisons and change.key in compiled
    )

    def compared(port: SnowflakePort, index: int) -> Change | Diagnostic:
        change = changes[index]
        comparison, artifact = comparisons[change.artifact_type], compiled[change.key]
        desired = comparison.desired(artifact)
        if desired is None:
            return change
        live = live_object(change, recorded.get(change.key))
        try:
            before = comparison.live(port, artifact, live)
        except SnowflakePortError as exc:
            return D("SST-PLN041", subject=change.key, artifact=change.key, value=live.name.sql, detail=str(exc))
        return replace(change, member_diff=diff_definitions(before, desired))

    results = dict(zip(comparable, readers.map(compared, comparable), strict=True))
    updated = tuple(
        result if isinstance(result := results.get(index, change), Change) else change
        for index, change in enumerate(changes)
    )
    refused = tuple(result for result in results.values() if isinstance(result, Diagnostic))
    return updated, refused


def compiled_fingerprints(
    compiled: CompiledArtifact, comparisons: Mapping[str, Comparison] = COMPARISONS
) -> tuple[tuple[str, str], ...]:
    """Return the member fingerprints of what `compiled` renders; none for a type plan does not compare.

    A definition that cannot be built, such as one naming a member twice, has none either, and so
    has an artifact that is not what its type's compiler builds.
    """
    comparison = comparisons.get(compiled.artifact_type)
    try:
        desired = comparison.desired(compiled) if comparison is not None else None
    except (TypeError, ValueError):
        return ()
    return member_fingerprints(desired) if desired is not None else ()


def from_state(changes: Sequence[Change], recorded: Mapping[str, AppliedEntry]) -> tuple[Change, ...]:
    """Name the members each update plan did not compare changes, from the fingerprints state records.

    An update with a member diff keeps it. One whose recorded entry, or whose render, holds no
    member fingerprints, such as an entry an earlier SST wrote, names none. An agent's or a CoWork
    profile's properties member is named only when its entry records one: an earlier SST recorded
    none, and naming it then would say the properties changed when nothing did.
    """
    return tuple(
        replace(
            change, changed_members=_recorded_changes(entry.member_fingerprints, change.rendered.member_fingerprints)
        )
        if change.action is Action.UPDATE
        and change.member_diff is None
        and change.rendered is not None
        and change.rendered.member_fingerprints
        and (entry := recorded.get(change.key)) is not None
        and entry.member_fingerprints
        else change
        for change in changes
    )


def _recorded_changes(before: Sequence[tuple[str, str]], after: Sequence[tuple[str, str]]) -> tuple[str, ...]:
    """The members whose recorded fingerprints differ, properties only when both sides record them."""
    if any(key in PROPERTIES_KEYS for key, _ in before):
        return changed_members(before, after)
    return changed_members(before, tuple(pair for pair in after if pair[0] not in PROPERTIES_KEYS))


def live_object(change: Change, recorded: AppliedEntry | None) -> LiveObject:
    """Where a planned update's object is: the one observed, else the one it publishes to.

    Raises:
        ValueError: the change names no object, which no update does.
    """
    observed, rendered = change.observed, change.rendered
    if observed is not None:
        name = observed.qualified_name
    elif rendered is not None:
        name = rendered.target
    else:
        raise ValueError(f"update {change.key} names no object")
    composite = change.composite_observation
    config_path = composite.config_path if composite is not None else ""
    return LiveObject(
        name,
        alias=rendered.desired_alias if rendered is not None else None,
        comment=observed.comment if observed is not None else None,
        routine_signature=observed.routine_signature if observed is not None else (),
        recorded=recorded,
        config_folder=config_path.rpartition("/")[0] if config_path else None,
    )
