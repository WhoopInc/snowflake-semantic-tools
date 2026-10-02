"""The entries a run leaves in state: what each change wrote, retired or kept, and what was observed.

State records every object SST wrote, even when the change then failed, so the next plan never
calls it unmanaged. A change that failed before writing keeps its previous entry, and an object
plan observed carrying SST's ownership marker is recorded as observed. The previous entries come
first, each outcome then replaces or retires its own, and an observed object is recorded only
when no entry names it yet.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOutcome,
    Change,
    ChangeSet,
    ObservedArtifact,
    OutcomeStatus,
    OwnershipMarker,
)
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.state import (
    APPLIED,
    DEACTIVATED,
    FAILED_AFTER_WRITE,
    AppliedEntry,
    AppliedResourceInput,
    State,
)


@dataclass(frozen=True, slots=True)
class EntryStamp:
    """What every entry a run writes records about the run.

    Attributes:
        applied_at: When the run finished, as the clock reported it.
        git_sha: The commit apply ran from; empty when it is unknown.
    """

    run_id: str
    applied_at: str
    git_sha: str


def _applied_after(
    changeset: ChangeSet,
    previous: State,
    outcomes: tuple[ApplyOutcome, ...],
    stamp: EntryStamp,
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler],
) -> dict[str, AppliedEntry]:
    """Return every entry state keeps after the run, by artifact key.

    Every previous entry is kept unless an outcome replaces or retires it. The outcomes are
    recorded in order, and the observed objects last, so an outcome's entry always wins.
    """
    applied = dict(previous.applied)
    by_key = {change.key: change for change in changeset.changes}
    failed_without_write = {
        outcome.key for outcome in outcomes if outcome.status is OutcomeStatus.FAILED and not outcome.write_succeeded
    }
    for outcome in outcomes:
        _record_outcome(
            applied, by_key[outcome.key], outcome, previous, changeset.manifest_id, stamp, lifecycle_handlers
        )
    _record_observed(applied, changeset.changes, previous, failed_without_write, stamp)
    return applied


def _record_outcome(
    applied: dict[str, AppliedEntry],
    change: Change,
    outcome: ApplyOutcome,
    previous: State,
    manifest_id: str,
    stamp: EntryStamp,
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler],
) -> None:
    """Record one outcome: a prune retires or keeps its entry; a write records what it published.

    A change that wrote nothing, or has nothing rendered, leaves the entries as they are.
    """
    if change.action is Action.PRUNE:
        _record_prune(applied, change, outcome, previous, manifest_id, stamp, lifecycle_handlers)
        return
    if outcome.status is not OutcomeStatus.APPLIED and not outcome.write_succeeded:
        return
    if change.rendered is None:
        return
    applied[change.key] = _written_entry(
        change,
        outcome,
        previous.applied.get(change.key),
        manifest_id,
        stamp,
        lifecycle_handlers.get(change.artifact_type),
    )


def _record_prune(
    applied: dict[str, AppliedEntry],
    change: Change,
    outcome: ApplyOutcome,
    previous: State,
    manifest_id: str,
    stamp: EntryStamp,
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler],
) -> None:
    """Record a prune: a report-only one keeps its entry, and an applied one retires it.

    A prune that ran but did not apply leaves the entry as it was.
    """
    if not change.prune_executable:
        # Nothing was removed, so the entry stays and the next plan reports it
        # again; it now belongs to this manifest, which is what clears SST-MAN021.
        retained = previous.applied.get(change.key)
        if retained is not None:
            applied[change.key] = replace(retained, manifest_id=manifest_id)
        return
    if outcome.status is not OutcomeStatus.APPLIED:
        return
    retired = previous.applied.get(change.key)
    if change.artifact_type in lifecycle_handlers and retired is not None:
        # A composite prune deactivates rather than drops, so the
        # object is still SST's: keep a tombstone to reactivate it.
        applied[change.key] = replace(retired, outcome=DEACTIVATED, applied_at=stamp.applied_at, run_id=stamp.run_id)
    else:
        applied.pop(change.key, None)


def _written_entry(
    change: Change,
    outcome: ApplyOutcome,
    previous_entry: AppliedEntry | None,
    manifest_id: str,
    stamp: EntryStamp,
    lifecycle_handler: CompositeLifecycleHandler | None,
) -> AppliedEntry:
    """Record what a change published: APPLIED, or FAILED_AFTER_WRITE when it wrote and then failed.

    Component fingerprints the outcome reports, as a composite handler does for the parts it
    verified, win over the rendered artifact's own.
    """
    assert change.rendered is not None
    return AppliedEntry(
        fingerprint=change.rendered.fingerprint,
        qualified_name=change.rendered.target.sql,
        applied_at=stamp.applied_at,
        run_id=stamp.run_id,
        outcome=(APPLIED if outcome.status is OutcomeStatus.APPLIED else FAILED_AFTER_WRITE),
        ddl_sha256=change.rendered.fingerprint,
        manifest_id=manifest_id,
        git_sha=stamp.git_sha,
        component_fingerprints=(outcome.component_fingerprints or change.rendered.component_fingerprints),
        physical_resources=_recorded_resources(change, outcome, previous_entry, lifecycle_handler),
    )


def _recorded_resources(
    change: Change,
    outcome: ApplyOutcome,
    previous_entry: AppliedEntry | None,
    lifecycle_handler: CompositeLifecycleHandler | None,
) -> tuple[AppliedResourceInput, ...]:
    """Return the physical resources state records for a written artifact.

    A composite handler reports exactly the resources it verified, so an empty report is
    authoritative rather than a cue to assume the rendered set; the handler then merges them
    with the previous entry's. Any other artifact records what the outcome reports, else the
    resources it was rendered with.
    """
    assert change.rendered is not None
    if lifecycle_handler is not None:
        return lifecycle_handler.merge_physical_resources(outcome.physical_resources, previous_entry)
    return outcome.physical_resources or tuple(
        (object_type, name.sql) for object_type, name in change.rendered.physical_resources
    )


def _record_observed(
    applied: dict[str, AppliedEntry],
    changes: tuple[Change, ...],
    previous: State,
    failed_without_write: set[str],
    stamp: EntryStamp,
) -> None:
    """Record the observed objects no entry names yet, when they carry SST's ownership marker.

    A change that failed without writing keeps its previous entry instead, or records nothing.
    """
    for change in changes:
        if change.key in applied or change.observed is None:
            continue
        if change.key in failed_without_write:
            if change.key in previous.applied:
                applied[change.key] = previous.applied[change.key]
            continue
        marker = change.observed.marker
        if marker is None:
            continue
        applied[change.key] = _observed_entry(change.observed, marker, stamp)


def _observed_entry(observed: ObservedArtifact, marker: OwnershipMarker, stamp: EntryStamp) -> AppliedEntry:
    """Record an object SST's marker claims, as the marker describes it."""
    return AppliedEntry(
        fingerprint=marker.fingerprint,
        qualified_name=observed.qualified_name.sql,
        applied_at=stamp.applied_at,
        run_id=stamp.run_id,
        outcome="observed",
        ddl_sha256=marker.fingerprint,
        manifest_id=marker.manifest_id,
        git_sha=stamp.git_sha,
        component_fingerprints=(),
        physical_resources=((observed.object_type, observed.qualified_name.sql),),
    )


def _run_outcome(outcomes: tuple[ApplyOutcome, ...]) -> str:
    """Name the run's outcome as state records it: `partial` when any change failed, else `ok`."""
    return "partial" if any(item.status is OutcomeStatus.FAILED for item in outcomes) else "ok"
