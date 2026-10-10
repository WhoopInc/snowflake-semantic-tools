"""Apply interrupted while its changes run: what finished is recorded, the rest skipped, the lock released.

The run is on the main thread, as the CLI's is, and each signal is sent to that thread alone
(`pthread_kill`) once a change is held in its statement, so it lands while the run waits. Every
wait is bounded: a signal that never arrived fails the test instead of hanging it.
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from snowflake_semantic_tools.cli.signals import terminated_as_interrupt
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, FailurePolicy, OutcomeStatus
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_sessions import ListPool, SlowSnowflake, apply_with
from tests.helpers.artifact_builders import change, changeset, rendered, state

pytestmark = pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="needs POSIX signals")


class Interrupter:
    """Once `entered` is set, send `signum` to the main thread `times` times, then release the change."""

    def __init__(self, entered: threading.Event, release: threading.Event, signum: int, times: int = 1) -> None:
        self._entered, self._release, self._signum, self._times = entered, release, signum, times
        self.sent = 0
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> Interrupter:
        self._thread.start()
        return self

    def _run(self) -> None:
        main = threading.main_thread().ident
        assert main is not None
        try:
            if self._entered.wait(timeout=5):
                for _ in range(self._times):
                    # Apart, so each lands in a later wait rather than in the handling of the last.
                    time.sleep(0.2)
                    signal.pthread_kill(main, self._signum)
                    self.sent += 1
                time.sleep(0.2)
        finally:
            self._release.set()

    def join(self) -> None:
        self._thread.join(timeout=10)
        assert not self._thread.is_alive()


@contextmanager
def ctrl_c_raises() -> Iterator[None]:
    """Give SIGINT Python's own handler, which a test runner may have replaced, for the block."""
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


def test_sigterm_records_the_running_change_skips_the_queued_one_and_releases_both_locks() -> None:
    release = threading.Event()
    port, store = SlowSnowflake(release), InMemoryStateStore()
    first, second = rendered("A"), rendered("B")
    interrupter = Interrupter(port.entered, release, signal.SIGTERM).start()

    with terminated_as_interrupt():
        result = apply_with(port, "run-a", store=store).run(
            changeset(change(first), change(second)), state(), ApplyOptions(parallelism=1)
        )
    interrupter.join()

    assert interrupter.sent == 1 and result.interrupted and not result.success
    assert [(outcome.key, outcome.status) for outcome in result.outcomes] == [
        (first.key, OutcomeStatus.APPLIED),
        (second.key, OutcomeStatus.SKIPPED),
    ]
    assert result.state_written and store.state is not None
    assert list(store.state.applied) == [first.key]
    assert not store.locked and port.run_locks.rows == {}
    assert len(port.scripts) == 1


def test_an_interrupt_under_fail_fast_records_the_change_it_waited_for() -> None:
    release = threading.Event()
    port = SlowSnowflake(release)
    first, second = rendered("A"), rendered("B")
    interrupter = Interrupter(port.entered, release, signal.SIGTERM).start()

    with terminated_as_interrupt():
        result = apply_with(port, "run-a").run(
            changeset(change(first), change(second)), state(), ApplyOptions(on_failure=FailurePolicy.STOP_ALL)
        )
    interrupter.join()

    assert result.interrupted
    assert [outcome.status for outcome in result.outcomes] == [OutcomeStatus.APPLIED, OutcomeStatus.SKIPPED]
    assert port.run_locks.rows == {}


def test_a_second_ctrl_c_halts_the_running_sessions_and_the_run_still_records_what_ended() -> None:
    release = threading.Event()
    main, worker = SlowSnowflake(), SlowSnowflake(release)
    worker.run_locks = main.run_locks
    pool = ListPool([worker])
    interrupter = Interrupter(worker.entered, release, signal.SIGINT, times=2).start()

    with ctrl_c_raises(), terminated_as_interrupt():
        result = apply_with(main, "run-a", sessions=pool).run(changeset(change(rendered("A"))), state())
    interrupter.join()

    assert interrupter.sent == 2 and pool.halted == "the run was interrupted"
    assert result.interrupted and result.state_written and main.run_locks.rows == {}


def test_a_third_ctrl_c_propagates_and_the_lock_is_still_released() -> None:
    release = threading.Event()
    port, store = SlowSnowflake(release), InMemoryStateStore()
    interrupter = Interrupter(port.entered, release, signal.SIGINT, times=3).start()

    with ctrl_c_raises(), terminated_as_interrupt(), pytest.raises(KeyboardInterrupt):
        apply_with(port, "run-a", store=store).run(changeset(change(rendered("A"))), state())
    interrupter.join()

    assert interrupter.sent == 3
    assert not store.locked and port.run_locks.rows == {}


def test_a_run_no_interrupt_reaches_is_not_marked_interrupted() -> None:
    port = SlowSnowflake()
    result = apply_with(port, "run-a").run(changeset(change(rendered("A"))), state())
    assert result.success and not result.interrupted
