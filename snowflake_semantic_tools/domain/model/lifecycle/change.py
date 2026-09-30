"""Plan's decisions: one `Change` per artifact, collected in the `ChangeSet` apply executes.

A change carries the rendered artifact as `for_action` narrowed it and the object plan
observed, so apply acts on exactly what plan saw. A composite lifecycle handler decides its
own artifact's action and reports it as a `CompositePlan`, which plan turns into a change.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..diagnostic import DiagnosticBag
from ..identifier import TargetIdentity
from .action import Action, ChangeReason
from .observation import ArtifactKey, CompositeObservation, ObservedArtifact
from .rendered import RenderedArtifact


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


@dataclass(frozen=True, slots=True)
class ChangeSet:
    """A plan: every change in dependency order, for one manifest and one target.

    Attributes:
        observation_at: When plan observed Snowflake, as the observation recorded it.
        plan_id: The content hash of the planned changes; empty when plan produced no
            changes to hash, as for a dependency cycle.
    """

    manifest_id: str
    target: TargetIdentity
    changes: tuple[Change, ...]
    diagnostics: DiagnosticBag
    observation_at: str
    full: bool = True
    plan_id: str = ""

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
