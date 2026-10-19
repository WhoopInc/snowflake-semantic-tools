"""Plan's decisions: one `Change` per artifact, collected in the `ChangeSet` apply executes.

A change carries the rendered artifact as `for_action` narrowed it and the object plan
observed, so apply acts on exactly what plan saw. A composite lifecycle handler decides its
own artifact's action and reports it as a `CompositePlan`, which plan turns into a change.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle.action import Action, ChangeReason
from snowflake_semantic_tools.domain.model.lifecycle.member import MemberDiff
from snowflake_semantic_tools.domain.model.lifecycle.observation import (
    ArtifactKey,
    CompositeObservation,
    ObservedArtifact,
)
from snowflake_semantic_tools.domain.model.lifecycle.rendered import RenderedArtifact


@dataclass(frozen=True, slots=True)
class CompositePlan:
    """The action a composite lifecycle handler chose for its artifact, and what it observed."""

    action: Action
    reason: ChangeReason
    observation: CompositeObservation
    diagnostics: DiagnosticBag = DiagnosticBag()


@dataclass(frozen=True, slots=True)
class Change:
    """What plan decided for one artifact, and what apply needs to carry it out.

    Attributes:
        rendered: The artifact with the statements its action runs; None for a prune, which
            drops what was observed.
        observed: The object plan observed under the key; None when there is none, and for a
            composite artifact, whose handler observes it instead.
        depends_on: The keys of the changes apply must finish first.
        order: The artifact type's position in DDL order, which orders independent changes;
            prunes run in reverse.
        composite_observation: What a composite handler observed; None for any other artifact.
        prune_executable: False for a prune plan reports but apply never executes.
        member_diff: What an update changes, member by member, live object against render; None
            when it was not compared, as for any other action, under `--no-diff`, or when the
            live definition could not be read.
        changed_members: For an update with no member diff, the members whose fingerprints differ
            from those state recorded, as `<kind>:<name>` keys; None when there is a member diff,
            or state recorded none to compare.
    """

    key: ArtifactKey
    artifact_type: str
    action: Action
    reason: ChangeReason
    rendered: RenderedArtifact | None
    observed: ObservedArtifact | None
    depends_on: tuple[ArtifactKey, ...]
    order: int
    diagnostics: DiagnosticBag = DiagnosticBag()
    composite_observation: CompositeObservation | None = None
    prune_executable: bool = True
    member_diff: tuple[MemberDiff, ...] | None = None
    changed_members: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class ChangeSet:
    """A plan: every change in dependency order, for one manifest and one target.

    Attributes:
        observation_at: When plan observed Snowflake, as the observation recorded it.
        plan_id: The content hash of the planned changes; empty when plan produced no
            changes to hash, as for a dependency cycle.
        schemas: The absent schemas apply creates before anything else, with
            `apply.create_schemas` on: those the writes, their upload stages and the state
            tables go in, in the order the plan first needs them.
    """

    manifest_id: str
    target: TargetIdentity
    changes: tuple[Change, ...]
    diagnostics: DiagnosticBag
    observation_at: str
    full: bool = True
    plan_id: str = ""
    schemas: tuple[SchemaScope, ...] = ()

    @property
    def writes(self) -> tuple[Change, ...]:
        """Return what apply would execute: creates, updates, and executable prunes, in plan order.

        A report-only prune is listed, never executed.
        """
        return tuple(
            change
            for change in self.changes
            if change.action in (Action.CREATE, Action.UPDATE)
            or (change.action is Action.PRUNE and change.prune_executable)
        )

    @property
    def report_only(self) -> tuple[Change, ...]:
        """Return the prunes plan reports but apply never executes, in plan order."""
        return tuple(change for change in self.changes if change.action is Action.PRUNE and not change.prune_executable)

    @property
    def blocked(self) -> tuple[Change, ...]:
        """Return the changes that cannot run, in plan order."""
        return tuple(change for change in self.changes if change.action is Action.BLOCKED)
