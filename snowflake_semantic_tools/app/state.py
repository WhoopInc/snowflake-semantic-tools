"""Reconcile authoritative remote state with the local cache."""

from __future__ import annotations

from types import MappingProxyType

from ..domain.model.diagnostic import D, DiagnosticBag
from ..domain.model.identifier import QualifiedName, TargetIdentity
from ..domain.ports.snowflake import SnowflakePort, StateStore
from ..domain.state.model import STATE_SCHEMA_VERSION, State


def read_state(
    store: StateStore,
    port: SnowflakePort | None,
    *,
    state_table: QualifiedName,
    target: TargetIdentity,
) -> tuple[State, DiagnosticBag]:
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

    remote_manifest_ids = {entry.manifest_id for entry in remote.values() if entry.manifest_id}
    remote_manifest_id = next(iter(remote_manifest_ids)) if len(remote_manifest_ids) == 1 else ""
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
