"""Read what each planned update replaces, and compare it member by member with what SST renders.

Plan decides an update only while the live object still matches what SST last wrote, so the live
definition is exactly what apply replaces. For each update of a type it can compare, this reads
that definition, builds the one SST compiled, and records their `member_diff` on the change. It
reads nothing for any other change, and a read Snowflake refuses only leaves the change without
its member diff (SST-PLN041): the plan is otherwise the same.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import TypeVar

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompiledView
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.plan_diff_composites import eval_sides, extension_sides, profile_sides
from snowflake_semantic_tools.app.prior import served_version
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change
from snowflake_semantic_tools.domain.model.tool import ToolKind
from snowflake_semantic_tools.domain.plan.definitions.agent import spec_definition
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import desired_view, live_view
from snowflake_semantic_tools.domain.plan.definitions.tool import (
    desired_routine,
    desired_search,
    live_routine,
    live_search,
    search_query,
)
from snowflake_semantic_tools.domain.plan.member_diff import Definition, diff_definitions
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry

# How one artifact type's two sides are read: (live, desired), from a session, what compiled, the
# change, and what state records of the artifact; None for an artifact of the type that is not
# compared, such as a stage tool.
DefinitionRead = Callable[
    [SnowflakePort, CompiledArtifact, Change, AppliedEntry | None], tuple[Definition, Definition] | None
]


def _semantic_view(
    port: SnowflakePort, compiled: CompiledArtifact, change: Change, _recorded: AppliedEntry | None
) -> tuple[Definition, Definition]:
    view = _compiled_as(compiled, CompiledView).view
    return live_view(port.describe_semantic_view(_replaced(change))), desired_view(view)


def _agent(
    port: SnowflakePort, compiled: CompiledArtifact, change: Change, _recorded: AppliedEntry | None
) -> tuple[Definition, Definition]:
    """The specification the version the agent serves holds, against the one SST renders."""
    agent = _compiled_as(compiled, CompiledAgent)
    alias = change.rendered.desired_alias if change.rendered is not None else None
    served = served_version(port.observe_agent_versions(_replaced(change)).versions, alias)
    if served is None or served.specification is None:
        raise SnowflakePortError("the role may not read the specification of the version the agent serves")
    try:
        live = spec_definition(served.specification)
    except ValueError as exc:
        raise SnowflakePortError(f"the specification the agent serves is not a JSON object: {exc}") from exc
    return live, spec_definition(agent.payload)


def _tool(
    port: SnowflakePort, compiled: CompiledArtifact, change: Change, _recorded: AppliedEntry | None
) -> tuple[Definition, Definition] | None:
    """A procedure, function or search service as DESCRIBE shows it, against its render; None for a stage."""
    tool = _compiled_as(compiled, CompiledTool)
    member, name = tool.member, _replaced(change)
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        described = port.describe_properties("CORTEX SEARCH SERVICE", name)
        if described is None:
            raise SnowflakePortError(f"no search service {name.sql} to describe")
        return live_search(member.name, described), desired_search(member, search_query(tool.rendered.ddl))
    if member.type not in (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value):
        return None
    procedure = member.type == ToolKind.PROCEDURE.value
    signature = tuple(parameter.type for parameter in member.signature)
    if change.observed is not None and change.observed.routine_signature:
        signature = change.observed.routine_signature
    described = port.describe_routine(member.type.upper(), name, signature)
    comment = change.observed.comment if change.observed is not None else None
    live = live_routine(member.name, described, comment, procedure=procedure)
    return live, desired_routine(member, procedure=procedure)


def _extension(
    port: SnowflakePort, compiled: CompiledArtifact, change: Change, _recorded: AppliedEntry | None
) -> tuple[Definition, Definition]:
    return extension_sides(port, compiled, change)


def _profile(
    port: SnowflakePort, compiled: CompiledArtifact, change: Change, _recorded: AppliedEntry | None
) -> tuple[Definition, Definition]:
    return profile_sides(port, compiled, change)


READS: Mapping[str, DefinitionRead] = {
    "semantic_view": _semantic_view,
    "agent": _agent,
    "tool": _tool,
    "skill": _extension,
    "plugin": _extension,
    "profile": _profile,
    "eval": eval_sides,
}


def diff_updates(
    changes: Sequence[Change],
    compiled: Mapping[str, CompiledArtifact],
    readers: Fanout[SnowflakePort],
    recorded: Mapping[str, AppliedEntry] = MappingProxyType({}),
    reads: Mapping[str, DefinitionRead] = READS,
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Return the changes with each comparable update's member diff, and a diagnostic per refused read.

    An update is comparable when `reads` has its artifact type and `compiled` its artifact. The
    reads run on `readers`, one item each, and the changes come back in their order; `recorded`
    is what state records of each artifact, which an eval's earlier publish is read from.

    Diagnostics:
        SST-PLN041: reading an update's live definition failed; the change has no member diff.
    """
    comparable = tuple(
        index
        for index, change in enumerate(changes)
        if change.action is Action.UPDATE and change.artifact_type in reads and change.key in compiled
    )

    def compared(port: SnowflakePort, index: int) -> Change | Diagnostic:
        change = changes[index]
        try:
            sides = reads[change.artifact_type](port, compiled[change.key], change, recorded.get(change.key))
        except SnowflakePortError as exc:
            name = _replaced(change).sql
            return D("SST-PLN041", subject=change.key, artifact=change.key, value=name, detail=str(exc))
        return change if sides is None else replace(change, member_diff=diff_definitions(*sides))

    results = dict(zip(comparable, readers.map(compared, comparable), strict=True))
    updated = tuple(
        result if isinstance(result := results.get(index, change), Change) else change
        for index, change in enumerate(changes)
    )
    refused = tuple(result for result in results.values() if isinstance(result, Diagnostic))
    return updated, refused


ArtifactT = TypeVar("ArtifactT", bound=CompiledArtifact)


def _compiled_as(compiled: CompiledArtifact, kind: type[ArtifactT]) -> ArtifactT:
    if not isinstance(compiled, kind):
        raise TypeError(f"{compiled.artifact_key} did not compile as a {kind.__name__}")
    return compiled


def _replaced(change: Change) -> QualifiedName:
    """The object an update replaces: the one observed, else the one it publishes to."""
    if change.observed is not None:
        return change.observed.qualified_name
    if change.rendered is None:
        raise ValueError(f"update {change.key} names no object")
    return change.rendered.target
