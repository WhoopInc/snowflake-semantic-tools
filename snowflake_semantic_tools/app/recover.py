"""Rebuild a target's lost state entries from what its live objects prove about themselves.

`RecoverState.run` is what `sst state recover` does once the target is planned. State that was
lost -- a dropped or recreated state table, a target pointed at a new one -- leaves every
object SST published unrecorded, so a plan blocks each as unmanaged (SST-PLN024). Recover
records, without writing any object, exactly those whose live objects prove they hold the
definition being planned:

- An object whose comment carries SST's ownership marker -- a semantic view, a tool, an agent
  -- when it is under the compiled name and its marker's fingerprint is the compiled
  fingerprint, the test plan already trusts for an apply stopped between publishing and
  recording (`_published_unrecorded`).
- A composite artifact, whose objects carry no marker, when its lifecycle handler proves from
  their content that they are the compiled definition (`CompositeLifecycleHandler.recover`):
  a skill or plugin by its aliased version's files and the bundle prefix they were added from,
  read back byte for byte; an eval by its dataset version's provenance and its staged config's
  bytes.

Each entry is the one apply records for that object, so the plan that follows reads them as
unchanged. Everything else that state does not record is reported with what the next apply
will do: an object with no marker stays unmanaged, one that holds another definition is
updated or blocked as the plan says, and an absent one is created. Recover rebuilds deploy
state only, never an eval's baselines. Every proof is read before the run lease is taken; the
lease is then held from before state is read again until the write commits, as apply holds
it, so a recover never races one.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.apply.state import EntryStamp, _observed_entry, _recovered_entry
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ABSENT,
    NO_MARKER,
    OTHER_DEFINITION,
    RECORDED,
    UNMARKED,
    UNOBSERVABLE,
    Change,
    ChangeReason,
    ChangeSet,
    RecoveryProof,
)
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite

_DEFAULT_LOCK_POLICY = LockPolicy()
_NO_HANDLERS: Mapping[str, CompositeLifecycleHandler] = MappingProxyType({})
# What the next apply does with an artifact recover left unrecorded, by the plan's action.
_NEXT_APPLY = {
    "create": "creates it",
    "update": "publishes it again",
    "blocked": "is blocked on it until it is dropped or renamed",
    "noop": "leaves it as it is",
    "prune": "drops it",
}


@dataclass(frozen=True, slots=True)
class RecoveredArtifact:
    """One artifact state did not record: what recover found, and what the next apply does.

    Attributes:
        found: `recorded`, or why not, as the values in `domain.model.lifecycle.recovery` name it.
        next_apply: The action the plan chose for it, which an apply after a recover takes for
            any artifact recover did not record; empty for one it recorded.
    """

    key: str
    found: str
    qualified_name: str
    next_apply: str = ""


@dataclass(frozen=True, slots=True)
class RecoverResult:
    """What a recover did: each unrecorded artifact, in key order, and whether it wrote state."""

    artifacts: tuple[RecoveredArtifact, ...]
    diagnostics: DiagnosticBag
    wrote: bool = False
    refused: bool = False

    @property
    def recorded(self) -> tuple[str, ...]:
        """Return the keys recover recorded."""
        return tuple(item.key for item in self.artifacts if item.found == RECORDED)


def classify(change: Change, state: State) -> RecoveredArtifact | None:
    """Say what an ownership marker proves for one planned artifact; None when state records it already.

    A composite artifact is its handler's to prove (`RecoverState.run`); without one, it is
    reported as carrying no marker on Snowflake.
    """
    if change.key in state.applied or change.rendered is None:
        return None
    rendered, observed = change.rendered, change.observed
    action = change.action.value
    if observed is None:
        found = ABSENT if change.composite_observation is None else UNMARKED
        return RecoveredArtifact(change.key, found, rendered.target.sql, action)
    if change.reason is not ChangeReason.UNMANAGED_OBJECT:
        return RecoveredArtifact(change.key, UNMARKED, observed.qualified_name.sql, action)
    marker = observed.marker
    if marker is None:
        return RecoveredArtifact(change.key, NO_MARKER, observed.qualified_name.sql, action)
    if observed.qualified_name.folded != rendered.target.folded or marker.fingerprint != rendered.fingerprint:
        return RecoveredArtifact(change.key, OTHER_DEFINITION, observed.qualified_name.sql, action)
    return RecoveredArtifact(change.key, RECORDED, observed.qualified_name.sql)


def _prove(
    change: Change, state: State, handlers: Mapping[str, CompositeLifecycleHandler]
) -> tuple[RecoveredArtifact, RecoveryProof | None] | None:
    """Say what recover finds for one planned artifact, with the proof a handler gave; None when recorded.

    A composite change goes to its handler whatever its action and reason, since dependency
    blocking overwrites the reason the handler planned. A read that fails proves nothing.
    """
    handler = handlers.get(change.artifact_type)
    rendered = change.rendered
    if change.composite_observation is None or handler is None or change.key in state.applied or rendered is None:
        item = classify(change, state)
        return None if item is None else (item, None)
    try:
        proof = handler.recover(change)
    except SnowflakePortError:
        proof = RecoveryProof(UNOBSERVABLE)
    name = proof.qualified_name or rendered.target.sql
    if proof.recorded:
        return RecoveredArtifact(change.key, RECORDED, name), proof
    return RecoveredArtifact(change.key, proof.found, name, change.action.value), None


class RecoverState:
    """Record, under the run lease, the planned artifacts whose live objects prove SST published them.

    Args:
        state_table: The target's state table, where the run lock also lives.
        actor, host: Who and where, as the run lock records them.
        git_sha: The commit recover ran from, which each recorded entry carries.
    """

    def __init__(
        self,
        port: StatePort,
        state_store: StateStore,
        clock: ClockPort,
        *,
        state_table: QualifiedName,
        actor: str = "",
        host: str = "",
        git_sha: str = "",
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._clock = clock
        self._state_table = state_table
        self._actor = actor
        self._host = host
        self._git_sha = git_sha
        self._lock_policy = lock_policy

    def run(
        self,
        changeset: ChangeSet,
        state: State,
        target_name: str,
        *,
        break_stale: bool = False,
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] = _NO_HANDLERS,
    ) -> RecoverResult:
        """Classify every unrecorded artifact, then record those proved, under the lease.

        Args:
            lifecycle_handlers: The composite artifact types' handlers, by type, which prove a
                composite artifact; one without a handler is not recoverable.

        Diagnostics:
            SST-APL011: another run holds the lock, or broke it before the write.
            SST-APL010: an expired lock was taken over, with `break_stale`.
            SST-APL032: what recover recorded, as a summary.
            SST-APL033: one per artifact recover could not record, with what the next apply does.
        """
        proofs: dict[str, RecoveryProof] = {}
        found: list[RecoveredArtifact] = []
        for change in changeset.changes:
            decided = _prove(change, state, lifecycle_handlers)
            if decided is not None:
                found.append(decided[0])
                if decided[1] is not None:
                    proofs[change.key] = decided[1]
        artifacts = tuple(sorted(found, key=lambda item: item.key))
        unrecoverable = tuple(_unrecoverable(item) for item in artifacts if item.found != RECORDED)
        if not any(item.found == RECORDED for item in artifacts):
            return RecoverResult(artifacts, DiagnosticBag((_summary(0, len(artifacts)), *unrecoverable)))
        run_id = self._clock.new_run_id()
        pending = _pending(changeset, artifacts, proofs, EntryStamp(run_id, self._clock.now_iso(), self._git_sha))
        lease = RunLease(
            self._state_store,
            self._port,
            self._state_table,
            target_name,
            LockClaim(run_id, self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
        )
        locked, reported = lease.acquire(break_stale=break_stale)
        if not locked or lease.fence is None:
            return RecoverResult(artifacts, DiagnosticBag((*reported, *unrecoverable)), refused=True)
        try:
            written = self._record(pending, frozenset(proofs), changeset.manifest_id, target_name, lease)
        finally:
            lease.release()
        if written is None:
            return RecoverResult(artifacts, DiagnosticBag((*reported, *unrecoverable)), refused=True)
        # A key another run recorded meanwhile is that run's, and stays out of the summary.
        artifacts = tuple(item for item in artifacts if item.found != RECORDED or item.key in written)
        summary = _summary(len(written), len(artifacts))
        return RecoverResult(artifacts, DiagnosticBag((*reported, summary, *unrecoverable)), wrote=bool(written))

    def _record(
        self,
        pending: Mapping[str, AppliedEntry],
        proved: frozenset[str],
        manifest_id: str,
        target_name: str,
        lease: RunLease,
    ) -> tuple[str, ...] | None:
        """Write the pending entries state still lacks, remote then local; None when the lock was lost.

        State is read again under the lease, so an entry an apply recorded since the plan stays.

        Args:
            proved: The keys a composite handler proved, which name no manifest of their own.
            manifest_id: The manifest planned, which state names when nothing else does.
        """
        assert lease.fence is not None
        current = self._port.read_state(self._state_table, target_name) or {}
        entries = {key: entry for key, entry in pending.items() if key not in current}
        if not entries:
            return ()
        marked = [entry for key, entry in entries.items() if key not in proved]
        manifest = self._port.read_state_manifest(self._state_table, target_name) or _common_manifest(
            marked, manifest_id
        )
        write = StateWrite(MappingProxyType(entries))
        if not self._port.write_state(self._state_table, target_name, manifest, write, lease.fence):
            return None
        cached = self._state_store.read_local()
        if cached is not None:
            kept = {**cached.applied, **{key: entry for key, entry in entries.items() if key not in cached.applied}}
            self._state_store.write_local(replace(cached, applied=MappingProxyType(kept)))
        return tuple(sorted(entries))


def _pending(
    changeset: ChangeSet,
    artifacts: tuple[RecoveredArtifact, ...],
    proofs: Mapping[str, RecoveryProof],
    stamp: EntryStamp,
) -> dict[str, AppliedEntry]:
    """Build the entry for each artifact recover recorded: from its handler's proof, else its marker."""
    changes = {change.key: change for change in changeset.changes}
    entries: dict[str, AppliedEntry] = {}
    for item in artifacts:
        if item.found != RECORDED:
            continue
        change = changes[item.key]
        proof = proofs.get(item.key)
        if proof is not None:
            assert change.rendered is not None
            entries[item.key] = _recovered_entry(change.rendered, proof, changeset.manifest_id, stamp)
            continue
        # Without a proof, only a marker for the compiled definition records an artifact (`classify`).
        observed = change.observed
        assert observed is not None and observed.marker is not None
        entries[item.key] = _observed_entry(observed, observed.marker, stamp)
    return entries


def _common_manifest(marked: list[AppliedEntry], fallback: str) -> str:
    """Return the manifest most recovered markers name, else `fallback`.

    A marker names the apply that last published its object; a proof names no manifest.
    """
    counted = Counter(entry.manifest_id for entry in marked).most_common(1)
    return counted[0][0] if counted else fallback


def _summary(recorded: int, unrecorded: int) -> Diagnostic:
    return D("SST-APL032", count=recorded, value=unrecorded - recorded)


def _unrecoverable(item: RecoveredArtifact) -> Diagnostic:
    return D("SST-APL033", artifact=item.key, found=item.found, value=_NEXT_APPLY.get(item.next_apply, item.next_apply))
