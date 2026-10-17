"""Rebuild a target's lost state entries from the ownership markers on its live objects.

`RecoverState.run` is what `sst state recover` does once the target is planned. State that was
lost -- a dropped or recreated state table, a target pointed at a new one -- leaves every
object SST published unrecorded, so a plan blocks each as unmanaged (SST-PLN024) though its
comment still carries SST's marker. Recover records, without writing any object, exactly those
whose marker proves they hold the definition being planned: the object is under the compiled
name, and its marker's fingerprint is the compiled fingerprint, the test plan already trusts
for an apply stopped between publishing and recording (`_published_unrecorded`). Each entry is
the one apply records for an observed object, so the plan that follows reads them as unchanged.

Everything else that state does not record is reported with what the next apply will do: an
object with no marker stays unmanaged, one whose marker names another definition is updated or
blocked as the plan says, an absent one is created, and a kind that carries no marker on
Snowflake (a profile, a skill, an eval) is published as new. The run lease is held from before
state is read again until the write commits, as apply holds it, so a recover never races one.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.apply.state import EntryStamp, _observed_entry
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Change, ChangeReason, ChangeSet
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite

# What recover found for one artifact state does not record.
RECORDED = "recorded"
NO_MARKER = "no marker"
OTHER_DEFINITION = "marker for another definition"
ABSENT = "absent"
UNMARKED = "no marker on Snowflake"
_DEFAULT_LOCK_POLICY = LockPolicy()
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
        found: `recorded`, `no marker`, `marker for another definition`, `absent`, or `no
            marker on Snowflake`.
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
    """Say what recover finds for one planned artifact; None when state records it already."""
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


class RecoverState:
    """Record, under the run lease, the planned objects whose markers prove SST published them.

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

    def run(self, changeset: ChangeSet, state: State, target_name: str, *, break_stale: bool = False) -> RecoverResult:
        """Classify every unrecorded artifact, then record those a marker proves, under the lease.

        Diagnostics:
            SST-APL011: another run holds the lock, or broke it before the write.
            SST-APL010: an expired lock was taken over, with `break_stale`.
            SST-APL032: what recover recorded, as a summary.
            SST-APL033: one per artifact recover could not record, with what the next apply does.
        """
        found = tuple(item for change in changeset.changes if (item := classify(change, state)) is not None)
        artifacts = tuple(sorted(found, key=lambda item: item.key))
        unrecoverable = tuple(_unrecoverable(item) for item in artifacts if item.found != RECORDED)
        changes = {change.key: change for change in changeset.changes}
        wanted = [item.key for item in artifacts if item.found == RECORDED]
        if not wanted:
            return RecoverResult(artifacts, DiagnosticBag((_summary(0, len(artifacts)), *unrecoverable)))
        run_id = self._clock.new_run_id()
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
            written = self._record(changes, wanted, target_name, run_id, lease)
        finally:
            lease.release()
        if written is None:
            return RecoverResult(artifacts, DiagnosticBag((*reported, *unrecoverable)), refused=True)
        # A key another run recorded meanwhile is that run's, and stays out of the summary.
        artifacts = tuple(item for item in artifacts if item.found != RECORDED or item.key in written)
        summary = _summary(len(written), len(artifacts))
        return RecoverResult(artifacts, DiagnosticBag((*reported, summary, *unrecoverable)), wrote=bool(written))

    def _record(
        self, changes: dict[str, Change], wanted: list[str], target_name: str, run_id: str, lease: RunLease
    ) -> tuple[str, ...] | None:
        """Write the entries of `wanted` state still lacks, remote then local; None when the lock was lost.

        State is read again under the lease, so an entry an apply recorded since the plan stays.
        """
        assert lease.fence is not None
        current = self._port.read_state(self._state_table, target_name) or {}
        stamp = EntryStamp(run_id, self._clock.now_iso(), self._git_sha)
        entries: dict[str, AppliedEntry] = {}
        for key in wanted:
            observed = changes[key].observed
            if key in current or observed is None or observed.marker is None:
                continue
            entries[key] = _observed_entry(observed, observed.marker, stamp)
        if not entries:
            return ()
        manifest = self._port.read_state_manifest(self._state_table, target_name) or _common_manifest(entries)
        write = StateWrite(MappingProxyType(entries))
        if not self._port.write_state(self._state_table, target_name, manifest, write, lease.fence):
            return None
        cached = self._state_store.read_local()
        if cached is not None:
            kept = {**cached.applied, **{key: entry for key, entry in entries.items() if key not in cached.applied}}
            self._state_store.write_local(replace(cached, applied=MappingProxyType(kept)))
        return tuple(sorted(entries))


def _common_manifest(entries: dict[str, AppliedEntry]) -> str:
    """Return the manifest most recovered markers name: the apply that last published them."""
    [(manifest, _)] = Counter(entry.manifest_id for entry in entries.values()).most_common(1)
    return manifest


def _summary(recorded: int, unrecorded: int) -> Diagnostic:
    return D("SST-APL032", count=recorded, value=unrecorded - recorded)


def _unrecoverable(item: RecoveredArtifact) -> Diagnostic:
    return D("SST-APL033", artifact=item.key, found=item.found, value=_NEXT_APPLY.get(item.next_apply, item.next_apply))
