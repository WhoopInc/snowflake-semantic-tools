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

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompiledView
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import desired_view, live_view
from snowflake_semantic_tools.domain.plan.member_diff import Definition, diff_definitions
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

# How one artifact type's two sides are read: (live, desired), from a session, what compiled, and
# the object the update replaces.
DefinitionRead = Callable[[SnowflakePort, CompiledArtifact, QualifiedName], tuple[Definition, Definition]]


def _semantic_view(
    port: SnowflakePort, compiled: CompiledArtifact, name: QualifiedName
) -> tuple[Definition, Definition]:
    if not isinstance(compiled, CompiledView):
        raise TypeError(f"{compiled.artifact_key} did not compile as a semantic view")
    return live_view(port.describe_semantic_view(name)), desired_view(compiled.view)


READS: Mapping[str, DefinitionRead] = {"semantic_view": _semantic_view}


def diff_updates(
    changes: Sequence[Change],
    compiled: Mapping[str, CompiledArtifact],
    readers: Fanout[SnowflakePort],
    reads: Mapping[str, DefinitionRead] = READS,
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Return the changes with each comparable update's member diff, and a diagnostic per refused read.

    An update is comparable when `reads` has its artifact type and `compiled` its artifact. The
    reads run on `readers`, one item each, and the changes come back in their order.

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
        name = _replaced(change)
        try:
            live, desired = reads[change.artifact_type](port, compiled[change.key], name)
        except SnowflakePortError as exc:
            return D("SST-PLN041", subject=change.key, artifact=change.key, value=name.sql, detail=str(exc))
        return replace(change, member_diff=diff_definitions(live, desired))

    results = dict(zip(comparable, readers.map(compared, comparable), strict=True))
    updated = tuple(
        result if isinstance(result := results.get(index, change), Change) else change
        for index, change in enumerate(changes)
    )
    refused = tuple(result for result in results.values() if isinstance(result, Diagnostic))
    return updated, refused


def _replaced(change: Change) -> QualifiedName:
    """The object an update replaces: the one observed, else the one it publishes to."""
    if change.observed is not None:
        return change.observed.qualified_name
    if change.rendered is None:
        raise ValueError(f"update {change.key} names no object")
    return change.rendered.target
