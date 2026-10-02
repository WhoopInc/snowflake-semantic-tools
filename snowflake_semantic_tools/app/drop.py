"""Drop one named object, break-glass: under the run lock, only when SST's marker says it is SST's.

`DropObject.run` is everything `sst drop` does once its command line is accepted. It takes the
target's run lease, the same local and remote lock apply holds, so a drop never races an apply.
It then refuses an object that does not exist, and one whose comment carries no SST ownership
marker: drop removes what SST published, never an object someone else owns under the same name.
Only then does it run the one DROP statement, and record the removal in state the way apply
records a prune: each entry naming the object is deleted from the state table in one write, and
from the local cache when one exists.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.apply.errors import classify_error
from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.signatures import signature_codes, snowflake_diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecutionError
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.sql import Sql, keyword, qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite

DROPPED, REJECTED, ABSENT, REFUSED = "dropped", "rejected", "absent", "refused"
_DEFAULT_LOCK_POLICY = LockPolicy()


class DropPort(CatalogPublicationPort, StatePort, Protocol):
    """What a drop needs: the catalog to find the object, a session to drop it, and state."""


@dataclass(frozen=True, slots=True)
class DropRequest:
    """The one object a drop removes, as its command line names it.

    Attributes:
        artifact_type: The registered type the operator says the object is.
        object_type: That type's Snowflake object type, which the DROP statement names.
        target_name: The dbt target the drop runs against, which the run lock is held for.
    """

    qualified_name: QualifiedName
    artifact_type: str
    object_type: str
    target_name: str

    @property
    def drop_sql(self) -> Sql:
        """Return the DROP statement the request runs."""
        return sql("DROP {kind} {name}", kind=keyword(self.object_type), name=qname(self.qualified_name))

    @property
    def statement(self) -> str:
        """Return the DROP statement as its text, as the audit and the JSON report show it."""
        return str(self.drop_sql)

    @property
    def artifact(self) -> str:
        """Return the artifact key diagnostics name the object by."""
        return artifact_key(self.artifact_type, self.qualified_name.name.value.casefold())


@dataclass(frozen=True, slots=True)
class DropResult:
    """What a drop did.

    Attributes:
        outcome: `dropped`, `rejected` (Snowflake refused the statement), `absent` (no such
            object), or `refused` (the lock is held, or the object is not SST's).
        forgotten: The state entries removed, by artifact key, in key order.
    """

    outcome: str
    diagnostics: DiagnosticBag
    forgotten: tuple[str, ...] = ()

    @property
    def dropped(self) -> bool:
        """Report whether the object was dropped."""
        return self.outcome == DROPPED


class DropObject:
    """Drop one object SST owns from one target, and forget it in state.

    Args:
        state_table: The target's state table, where the run lock also lives.
        actor, host: Who and where, as the run lock records them.
    """

    def __init__(
        self,
        port: DropPort,
        state_store: StateStore,
        clock: ClockPort,
        *,
        state_table: QualifiedName,
        actor: str = "",
        host: str = "",
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._clock = clock
        self._state_table = state_table
        self._actor = actor
        self._host = host
        self._lock_policy = lock_policy

    def run(self, request: DropRequest) -> DropResult:
        """Take the run lease, check the object, drop it, and forget it in state.

        The lease is released however the run ends. Nothing is dropped while another run holds
        either lock.

        Raises:
            SnowflakePortError: the lock, the catalog, or the state could not be read or written.

        Diagnostics:
            SST-APL011: another run holds the lock.
            SST-APL010: an expired lock was taken over.
            SST-PRT005: the named object does not exist.
            SST-PLN024: the object carries no SST ownership marker, so it is not SST's to drop.
            SST-APL001: Snowflake refused the DROP; with the refusal under its SNO code.
        """
        lease = RunLease(
            self._state_store,
            self._port,
            self._state_table,
            request.target_name,
            LockClaim(self._clock.new_run_id(), self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
        )
        locked, reported = lease.acquire(break_stale=False)
        if not locked:
            return DropResult(REFUSED, DiagnosticBag(reported))
        try:
            result = self._run_locked(request)
        finally:
            lease.release()
        return replace(result, diagnostics=DiagnosticBag((*reported, *result.diagnostics)))

    def _run_locked(self, request: DropRequest) -> DropResult:
        name = request.qualified_name
        if not self._port.object_exists(request.object_type, name):
            detail = f"no {request.object_type} of that name"
            absent = D("SST-PRT005", subject=request.artifact, value=name.sql, detail=detail)
            return DropResult(ABSENT, DiagnosticBag((absent,)))
        if self._port.describe_marker(name, request.object_type) is None:
            unowned = D("SST-PLN024", subject=request.artifact, artifact=request.artifact, value=name.sql)
            return DropResult(REFUSED, DiagnosticBag((unowned,)))
        executed = self._port.execute_script((request.drop_sql,))
        if not executed.ok:
            return DropResult(REJECTED, DiagnosticBag(_rejection(request, executed.error)))
        return DropResult(DROPPED, DiagnosticBag(), self._forget(request))

    def _forget(self, request: DropRequest) -> tuple[str, ...]:
        """Delete every state entry that records the dropped object, remote first, then local."""
        entries = self._port.read_state(self._state_table, request.target_name) or {}
        keys = _recording(entries, request)
        if not keys:
            return ()
        manifest = self._port.read_state_manifest(self._state_table, request.target_name)
        self._port.write_state(
            self._state_table,
            request.target_name,
            manifest or entries[keys[0]].manifest_id,
            StateWrite(MappingProxyType({}), keys),
        )
        cached = self._state_store.read_local()
        if cached is not None and any(key in cached.applied for key in keys):
            kept = {key: entry for key, entry in cached.applied.items() if key not in keys}
            self._state_store.write_local(replace(cached, applied=MappingProxyType(kept)))
        return keys


def _recording(entries: Mapping[str, AppliedEntry], request: DropRequest) -> tuple[str, ...]:
    """Return the keys of the entries of the request's type that name its object, sorted."""
    folded = request.qualified_name.folded
    return tuple(
        sorted(
            key
            for key, entry in entries.items()
            if split_artifact_key(key)[0] == request.artifact_type and _folded(entry.qualified_name) == folded
        )
    )


def _folded(name: str) -> object:
    try:
        return QualifiedName.parse(name).folded
    except ValueError:
        return None


def _rejection(request: DropRequest, error: ExecutionError | None) -> tuple[Diagnostic, ...]:
    """Report a DROP Snowflake refused: as an apply failure, and under its SNO code when it has one."""
    message = error.message if error is not None and error.message else "the statement failed"
    classified = classify_error(
        message,
        sqlstate=error.sqlstate if error is not None else None,
        errno=error.errno if error is not None else None,
    )
    failed = D("SST-APL001", artifact=request.artifact, value="drop", detail=message)
    if classified.code not in signature_codes():
        return (failed,)
    return (
        failed,
        snowflake_diagnostic(classified.code, message, value=request.qualified_name.sql, subject=request.artifact),
    )
