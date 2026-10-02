"""Reconcile authoritative remote state with the local cache, and summarise what a plan found changed.

`read_state` returns the state a run starts from; `change_summary` counts, per artifact type,
what change detection found against it.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeSet
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import (
    DEACTIVATED,
    SST_VERSION,
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    State,
    written_by_newer,
)


def read_state(
    store: StateStore,
    port: StatePort | None,
    *,
    state_table: QualifiedName,
    target: TargetIdentity,
) -> tuple[State, DiagnosticBag]:
    """Return the state a run starts from: the state table's, or offline the local cache's.

    The local cache is read first, online too, and is set aside when it was written for another
    schema under this target's name. Online, the state table is authoritative: a
    cache whose entries disagree with it is rewritten, while a missing cache stays missing,
    and the state's manifest is the one the last state write recorded, else the one the
    active entries name, empty when they name none or several.

    Args:
        port: The connection; None reads offline, from the local cache alone.

    Returns:
        The state, with what reading it reported. Offline it is the cache as stored; online
        it has no `last_run`. It is empty when offline there is no cache, or online the state
        table is absent or cannot be read.

    Raises:
        ProjectError: the local cache exists and cannot be used, as `StateStore.read_local` raises.
        SnowflakePortError: checking for the state table or reading its manifest failed, or an
            entry it holds does not decode.

    Diagnostics:
        SST-MAN025: the local cache was written for another database or schema under this
            target's file name; it is set aside, and neither read nor rewritten.
        SST-MAN024: the local cache was written by a later SST release.
        SST-MAN020: offline, there is no local cache; every artifact reads as new.
        SST-PLN001: the state table exists and cannot be read.
        SST-MAN027: the local cache disagreed with the state table, and was rewritten.
    """
    diagnostics: list[Diagnostic] = []
    cached = store.read_local()
    if cached is not None:
        notes, cached = _checked_cache(cached, store, target)
        diagnostics.extend(notes)
        if cached is None:
            # Another writer's file: the run must neither read it nor write over it.
            return _from_table(port, state_table, target, store, diagnostics, rewrite=False)
    if port is None:
        if cached is None:
            diagnostics.append(D("SST-MAN020"))
            return State.empty(target, store.config_path), DiagnosticBag(diagnostics)
        return cached, DiagnosticBag(diagnostics)
    return _from_table(port, state_table, target, store, diagnostics, cached=cached)


def _checked_cache(
    cached: State, store: StateStore, target: TargetIdentity
) -> tuple[tuple[Diagnostic, ...], State | None]:
    """Check the cache was written for this target by this or an earlier SST; None sets it aside.

    Diagnostics:
        SST-MAN025: the cache records another database or schema than the target's.
        SST-MAN024: the cache was written by a later SST release.
    """
    recorded = cached.target
    if (recorded.database.folded, recorded.schema.folded) != (target.database.folded, target.schema.folded):
        writer = f"target {recorded.name} for {recorded.scope.sql}"
        return (D("SST-MAN025", path=store.location, value=writer),), None
    if cached.last_run is not None and written_by_newer(cached.last_run.sst_version, SST_VERSION):
        newer = D("SST-MAN024", path=store.location, found=cached.last_run.sst_version, expected=SST_VERSION)
        return (newer,), cached
    return (), cached


def _from_table(
    port: StatePort | None,
    state_table: QualifiedName,
    target: TargetIdentity,
    store: StateStore,
    diagnostics: list[Diagnostic],
    *,
    cached: State | None = None,
    rewrite: bool = True,
) -> tuple[State, DiagnosticBag]:
    """Read the state table, rewriting a cache that disagrees with it unless `rewrite` is False.

    Offline, with no cache to read, the state is empty.

    Diagnostics:
        SST-PLN001: the state table exists and cannot be read.
        SST-MAN027: the local cache disagreed with the state table, and was rewritten.
    """
    if port is None:
        return State.empty(target, store.config_path), DiagnosticBag(diagnostics)
    remote = port.read_state(state_table, target.name)
    if remote is None:
        diagnostics.append(
            D(
                "SST-PLN001",
                value=state_table.sql,
                detail="authoritative state table is unreadable",
            )
        )
        return State.empty(target, store.config_path), DiagnosticBag(diagnostics)

    remote_manifest_id = port.read_state_manifest(state_table, target.name) or _entries_manifest(remote)
    remote_state = State(
        STATE_SCHEMA_VERSION,
        target,
        remote_manifest_id,
        store.config_path,
        None,
        MappingProxyType(dict(remote)),
    )
    if rewrite and cached is not None and cached.applied != remote_state.applied:
        diagnostics.append(D("SST-MAN027", value=target.name, detail=state_table.sql))
        store.write_local(remote_state)
    return remote_state, DiagnosticBag(diagnostics)


def _entries_manifest(entries: Mapping[str, AppliedEntry]) -> str:
    """Return the one manifest the active entries name, else empty: for a table that records none."""
    named = {entry.manifest_id for entry in entries.values() if entry.manifest_id and entry.outcome != DEACTIVATED}
    return next(iter(named)) if len(named) == 1 else ""


# How change detection names each action it counts; a blocked change is none of these.
_DETECTED = (
    (Action.CREATE, "new"),
    (Action.UPDATE, "modified"),
    (Action.NOOP, "unmodified"),
    (Action.PRUNE, "orphaned"),
)


def change_summary(changeset: ChangeSet) -> tuple[Diagnostic, ...]:
    """Count, per artifact type in name order, the new, modified, unmodified and orphaned artifacts.

    Diagnostics:
        SST-MAN026: one summary line per plan with any change; none for an empty plan.
    """
    if not changeset.changes:
        return ()
    types = sorted({change.artifact_type for change in changeset.changes})
    lines = []
    for artifact_type in types:
        actions = [change.action for change in changeset.changes if change.artifact_type == artifact_type]
        counts = ", ".join(f"{actions.count(action)} {label}" for action, label in _DETECTED)
        lines.append(f"{artifact_type}: {counts}")
    return (D("SST-MAN026", value="; ".join(lines)),)
