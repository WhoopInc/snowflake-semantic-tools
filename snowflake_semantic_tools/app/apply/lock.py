"""The run lease: the local cache lock and the authoritative lock in Snowflake, held for one run.

`RunLease.acquire` takes the local lock first, which is cheap and guards this machine, then
the remote lock, which guards the target against every machine; refusing either releases the
other. The remote claim returns the run's `LockFence`, which every extension, release, and
state write presents. While held, a heartbeat thread extends the remote lock so a long apply
never expires under its own run; it runs on a session of its own when one is given, so it
never queues behind the run's statements.

The heartbeat marks the lease lost, and calls `on_lost` at once, when an extension finds the
lock broken, when extensions keep failing until the lock could expire before the next one, or
when anything else goes wrong in it: it never dies silently. Its waiting and its clock come
from a `Ticker`, so a test steps beats one at a time. `release` stops the heartbeat, joins it
for at most `LockPolicy.stop_seconds`, and releases both locks, remote first.
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state.lock import LOCK_HEARTBEAT_SECONDS, LOCK_TTL_SECONDS, LockClaim, LockFence

# What a lost lease reports as the run that holds the lock, in SST-APL011.
BROKEN_BY_ANOTHER_RUN = "another run, which broke this run's lock"


class Ticker(Protocol):
    """When a heartbeat beats: the wait between two beats, the stop that ends it, and elapsed time."""

    def wait(self, seconds: float) -> bool:
        """Wait `seconds`, or until `stop`; return True when stopped, before or during the wait."""
        ...

    def stop(self) -> None:
        """End every wait, now and later; idempotent."""
        ...

    def monotonic(self) -> float:
        """Return a monotonic reading in seconds; only differences between readings mean anything."""
        ...


class EventTicker:
    """The real ticker: waits on an event, and reads the process's monotonic clock."""

    def __init__(self) -> None:
        self._stopped = threading.Event()

    def wait(self, seconds: float) -> bool:
        """Wait `seconds` on the stop event; True when it was set."""
        return self._stopped.wait(seconds)

    def stop(self) -> None:
        """Set the stop event, ending the current wait and every later one."""
        self._stopped.set()

    def monotonic(self) -> float:
        """Return the process's monotonic clock, in seconds."""
        return time.monotonic()


@dataclass(frozen=True, slots=True)
class LockPolicy:
    """How long the remote lock lasts unextended, how often the heartbeat extends it, and how it is timed.

    Attributes:
        ttl_seconds: The lock expires this long after it was taken or last extended.
        heartbeat_seconds: The heartbeat's interval; it must be well below the time to live.
        stop_seconds: How long `release` waits for the heartbeat to stop, should an extension
            be stuck in a driver call; the thread is a daemon, and is then left behind.
        ticker: Makes each lease's ticker.
    """

    ttl_seconds: int = LOCK_TTL_SECONDS
    heartbeat_seconds: float = LOCK_HEARTBEAT_SECONDS
    stop_seconds: float = 60.0
    ticker: Callable[[], Ticker] = field(default=EventTicker)


class RunLease:
    """Both locks one run holds on a target, and the heartbeat that keeps the remote one alive.

    `lost` turns True as soon as the heartbeat finds that the run no longer holds the remote
    lock, or can no longer prove it will; `lost_reason` then says why, and `on_lost` was called
    with it. `holder` names the run that held a lock this lease was refused, and `fence` is the
    run's fencing token while it holds the remote lock.

    Args:
        heartbeat_port: Where the heartbeat extends the lock: a session no other statement of
            the run uses. None extends on `port`.
        on_lost: Called once, from the heartbeat thread, the moment the lease is lost; it must
            not block. What it raises is ignored: the lease is lost regardless.
    """

    def __init__(
        self,
        store: StateStore,
        port: StatePort,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        policy: LockPolicy,
        *,
        heartbeat_port: StatePort | None = None,
        on_lost: Callable[[str], None] | None = None,
    ) -> None:
        self._store = store
        self._port = port
        self._heartbeat_port = heartbeat_port or port
        self._state_table = state_table
        self._target_name = target_name
        self._claim = claim
        self._policy = policy
        self._on_lost = on_lost
        self._ticker = policy.ticker()
        self._lost = threading.Event()
        self._lost_reason: str | None = None
        self._heartbeat: threading.Thread | None = None
        self._holder: str | None = None
        self._fence: LockFence | None = None

    @property
    def holder(self) -> str | None:
        """The run id holding the lock the last `acquire` was refused; None when it was not, or is unknown."""
        return self._holder

    @property
    def fence(self) -> LockFence | None:
        """The run's fencing token while it holds the remote lock; None before `acquire` succeeds."""
        return self._fence

    @property
    def lost(self) -> bool:
        """Report whether the run can no longer count on holding the remote lock."""
        return self._lost.is_set()

    @property
    def lost_reason(self) -> str:
        """Who holds the lock now, as SST-APL011 names it, once the lease is lost."""
        return self._lost_reason or BROKEN_BY_ANOTHER_RUN

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
        if not remote.acquired or remote.fence is None:
            self._store.release_lock(run_id)
            self._holder = remote.holder.run_id if remote.holder is not None else None
            value = remote.holder.describe() if remote.holder is not None else "another run"
            return False, (*reported, D("SST-APL011", value=value))
        if remote.broke_stale and remote.holder is not None:
            reported.append(D("SST-APL010", value=remote.holder.describe()))
        self._fence = remote.fence
        self._start_heartbeat(remote.fence)
        return True, tuple(reported)

    def release(self) -> None:
        """Stop the heartbeat, then release the remote lock and the local one; idempotent.

        The local lock is released even when releasing the remote one raises, which then
        propagates; the remote lock then expires on its own. A lease that never held the remote
        lock releases only the local one.
        """
        self._ticker.stop()
        if self._heartbeat is not None:
            self._heartbeat.join(self._policy.stop_seconds)
            self._heartbeat = None
        fence, self._fence = self._fence, None
        try:
            if fence is not None:
                self._port.release_run_lock(self._state_table, self._target_name, fence)
        finally:
            self._store.release_lock(self._claim.run_id)

    def _start_heartbeat(self, fence: LockFence) -> None:
        self._heartbeat = threading.Thread(
            target=self._beat, args=(fence,), name=f"sst-lock-{self._claim.run_id}", daemon=True
        )
        self._heartbeat.start()

    def _beat(self, fence: LockFence) -> None:
        """Extend the remote lock every interval until stopped, or until the lease is lost.

        A failed extension is retried at the next interval while the lock is sure to outlive
        it; once it is not, or on anything but a port error, the lease is lost.
        """
        interval, ttl = self._policy.heartbeat_seconds, self._policy.ttl_seconds
        extended = self._ticker.monotonic()
        try:
            while not self._ticker.wait(interval):
                try:
                    held = self._heartbeat_port.extend_run_lock(self._state_table, self._target_name, fence, ttl)
                except SnowflakePortError as exc:
                    if self._ticker.monotonic() - extended + interval < ttl:
                        continue
                    self._lose(f"another run, perhaps: the lock may have expired, since extending it failed ({exc})")
                    return
                if not held:
                    self._lose(BROKEN_BY_ANOTHER_RUN)
                    return
                extended = self._ticker.monotonic()
        except BaseException as exc:
            self._lose(f"another run, perhaps: this run's lock heartbeat failed ({type(exc).__name__}: {exc})")
        finally:
            self._ticker.stop()

    def _lose(self, reason: str) -> None:
        """Mark the lease lost with `reason`, then tell `on_lost` at once."""
        self._lost_reason = reason
        self._lost.set()
        if self._on_lost is not None:
            # The lease is lost whatever the callback does, and the heartbeat must not die of it.
            with contextlib.suppress(Exception):
                self._on_lost(reason)
