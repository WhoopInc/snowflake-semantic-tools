"""The run lease: the local cache lock and the authoritative lock in Snowflake, held for one run.

`RunLease.acquire` takes the local lock first, which is cheap and guards this machine, then
the remote lock, which guards the target against every machine; refusing either releases the
other. While held, a heartbeat thread extends the remote lock so a long apply never expires
under its own run. `release` stops the heartbeat and releases both locks, remote first.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state.lock import LOCK_HEARTBEAT_SECONDS, LOCK_TTL_SECONDS, LockClaim


@dataclass(frozen=True, slots=True)
class LockPolicy:
    """How long the remote lock lasts unextended, and how often the heartbeat extends it.

    Attributes:
        ttl_seconds: The lock expires this long after it was taken or last extended.
        heartbeat_seconds: The heartbeat's interval; it must be well below the time to live.
    """

    ttl_seconds: int = LOCK_TTL_SECONDS
    heartbeat_seconds: float = LOCK_HEARTBEAT_SECONDS


class RunLease:
    """Both locks one run holds on a target, and the heartbeat that keeps the remote one alive.

    `lost` turns True when a heartbeat finds that another run broke the remote lock, and
    `holder` names the run that held a lock this lease was refused.
    """

    def __init__(
        self,
        store: StateStore,
        port: StatePort,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        policy: LockPolicy,
    ) -> None:
        self._store = store
        self._port = port
        self._state_table = state_table
        self._target_name = target_name
        self._claim = claim
        self._policy = policy
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._heartbeat: threading.Thread | None = None
        self._holder: str | None = None

    @property
    def holder(self) -> str | None:
        """The run id holding the lock the last `acquire` was refused; None when it was not, or is unknown."""
        return self._holder

    @property
    def lost(self) -> bool:
        """Report whether a heartbeat found that another run took the remote lock over."""
        return self._lost.is_set()

    def acquire(self, *, break_stale: bool) -> tuple[bool, tuple[Diagnostic, ...]]:
        """Take the local lock, then the remote one, and start the heartbeat once both are held.

        Returns:
            Whether the run holds both locks, with what taking them reported.

        Raises:
            SnowflakePortError: the remote lock could not be read or claimed; the local lock
                is released first.

        Diagnostics:
            SST-APL011: another run holds either lock, or the remote lock expired and
                `break_stale` was not given.
            SST-APL010: an expired lock, local or remote, was taken over.
        """
        run_id = self._claim.run_id
        self._holder = None
        locked, holder, broke_local = self._store.acquire_lock(run_id, break_stale=break_stale)
        if not locked:
            self._holder = holder
            return False, (D("SST-APL011", value=holder or "another run"),)
        reported = [D("SST-APL010", value=holder or "expired run")] if broke_local else []
        try:
            remote = self._port.acquire_run_lock(
                self._state_table, self._target_name, self._claim, break_stale=break_stale
            )
        except BaseException:
            self._store.release_lock(run_id)
            raise
        if not remote.acquired:
            self._store.release_lock(run_id)
            self._holder = remote.holder.run_id if remote.holder is not None else None
            value = remote.holder.describe() if remote.holder is not None else "another run"
            return False, (*reported, D("SST-APL011", value=value))
        if remote.broke_stale and remote.holder is not None:
            reported.append(D("SST-APL010", value=remote.holder.describe()))
        self._start_heartbeat()
        return True, tuple(reported)

    def release(self) -> None:
        """Stop the heartbeat, then release the remote lock and the local one; idempotent.

        The local lock is released even when releasing the remote one raises, which then
        propagates; the remote lock then expires on its own.
        """
        self._stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join()
            self._heartbeat = None
        try:
            self._port.release_run_lock(self._state_table, self._target_name, self._claim.run_id)
        finally:
            self._store.release_lock(self._claim.run_id)

    def _start_heartbeat(self) -> None:
        self._heartbeat = threading.Thread(target=self._beat, name=f"sst-lock-{self._claim.run_id}", daemon=True)
        self._heartbeat.start()

    def _beat(self) -> None:
        """Extend the remote lock every interval until stopped, or until the lock is found lost.

        A failed extension is retried at the next interval: the lock still has most of its
        time to live, and the run's own statements report a lost connection.
        """
        while not self._stop.wait(self._policy.heartbeat_seconds):
            try:
                held = self._port.extend_run_lock(self._state_table, self._target_name, self._claim)
            except SnowflakePortError:
                continue
            if not held:
                self._lost.set()
                return
