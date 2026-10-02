"""Reconcile authoritative remote state with the local cache."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import DEACTIVATED, STATE_SCHEMA_VERSION, AppliedEntry, State


def read_state(
    store: StateStore,
    port: StatePort | None,
    *,
    state_table: QualifiedName,
    target: TargetIdentity,
) -> tuple[State, DiagnosticBag]:
    """Return the state a run starts from: the state table's, or offline the local cache's.

    The local cache is read first, online too. Online, the state table is authoritative: a
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
        SST-MAN020: offline, there is no local cache; every artifact reads as new.
        SST-PLN001: the state table exists and cannot be read.
        SST-MAN027: the local cache disagreed with the state table, and was rewritten.
    """
    diagnostics = []
    cached = store.read_local()
    if port is None:
        if cached is None:
            diagnostics.append(D("SST-MAN020"))
            return State.empty(target, store.config_path), DiagnosticBag(diagnostics)
        return cached, DiagnosticBag()

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
    if cached is not None and cached.applied != remote_state.applied:
        diagnostics.append(D("SST-MAN027", value=target.name, detail=state_table.sql))
        store.write_local(remote_state)
    return remote_state, DiagnosticBag(diagnostics)


def _entries_manifest(entries: Mapping[str, AppliedEntry]) -> str:
    """Return the one manifest the active entries name, else empty: for a table that records none."""
    named = {entry.manifest_id for entry in entries.values() if entry.manifest_id and entry.outcome != DEACTIVATED}
    return next(iter(named)) if len(named) == 1 else ""
