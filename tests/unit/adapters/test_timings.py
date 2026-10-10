"""The run's timings: what the recorder counts, from how many threads, and what records into it."""

from __future__ import annotations

import contextlib
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from snowflake.connector.cursor import SnowflakeCursor

from snowflake_semantic_tools.adapters.dbt.invoke import subprocess_runner
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.adapters.snowflake.connector import session as session_module
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS, RunTimings
from snowflake_semantic_tools.domain.sql import sql
from tests.helpers.snowflake_fake.driver import FakeDriverSession


class _Ticks:
    """A clock that reads the next of the times it was given."""

    def __init__(self, *times: float) -> None:
        self._times = list(times)

    def __call__(self) -> float:
        return self._times.pop(0)


@pytest.fixture
def recording() -> Iterator[RunTimings]:
    """The process's recorder, on, and reset when the test ends."""
    RUN_TIMINGS.reset()
    RUN_TIMINGS.enable()
    yield RUN_TIMINGS
    RUN_TIMINGS.reset()


def test_a_recorder_switched_off_records_nothing_and_its_hooks_are_null() -> None:
    timings = RunTimings(_Ticks())
    hooks = (timings.phase("load"), timings.round_trip(), timings.login(), timings.dbt_process())
    assert all(isinstance(hook, contextlib.nullcontext) for hook in hooks)
    for hook in hooks:
        with hook:
            pass
    assert not timings.enabled
    assert timings.snapshot() == RunTimings().snapshot()


def test_round_trips_logins_dbt_and_phases_count_however_the_block_ends() -> None:
    timings = RunTimings(_Ticks(0, 2, 10, 11, 20, 23, 30, 31, 40, 44, 50, 52), enabled=True)
    with timings.round_trip():
        pass
    with pytest.raises(RuntimeError), timings.round_trip():
        raise RuntimeError("a failed statement still went to Snowflake")
    with timings.login():
        pass
    with timings.dbt_process():
        pass
    with timings.phase("load"):
        pass
    with timings.phase("load"):
        pass
    assert timings.snapshot() == {
        "phases": {"load": 6.0},
        "snowflake": {"queries": 2, "client_s": 3.0, "max_s": 2.0, "busy_s": 6.0, "connects": 1, "connect_s": 3.0},
        "dbt_runs": 1,
        "dbt_s": 1.0,
    }


def test_overlapping_statements_add_up_as_client_time_but_once_as_busy_time() -> None:
    timings = RunTimings(_Ticks(0, 1, 4, 6), enabled=True)
    first, second = timings.round_trip(), timings.round_trip()
    first.__enter__()
    second.__enter__()
    first.__exit__(None, None, None)
    second.__exit__(None, None, None)
    snowflake = cast(dict[str, float], timings.snapshot()["snowflake"])
    assert (snowflake["client_s"], snowflake["busy_s"], snowflake["max_s"]) == (9.0, 6.0, 5.0)


def test_a_call_in_flight_across_a_reset_counts_in_neither_run() -> None:
    timings = RunTimings(_Ticks(0, 1, 1.5, 2, 3, 3.5, 5, 6), enabled=True)
    statement, phase, dbt = timings.round_trip(), timings.phase("observe"), timings.dbt_process()
    statement.__enter__()
    phase.__enter__()
    dbt.__enter__()
    timings.reset()
    timings.enable()
    statement.__exit__(None, None, None)
    phase.__exit__(None, None, None)
    dbt.__exit__(None, None, None)
    with timings.round_trip():
        pass
    snowflake = cast(dict[str, float], timings.snapshot()["snowflake"])
    assert timings.snapshot()["phases"] == {} and timings.snapshot()["dbt_runs"] == 0
    assert (snowflake["queries"], snowflake["busy_s"]) == (1, 1.0)


def test_threads_recording_at_once_lose_no_count() -> None:
    timings = RunTimings(enabled=True)
    threads, each = 8, 400
    start = threading.Barrier(threads)

    def work() -> None:
        start.wait()
        for _ in range(each):
            with timings.round_trip(), timings.phase("publish"):
                pass
            with timings.login():
                pass

    workers = [threading.Thread(target=work) for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    snowflake = cast(dict[str, float], timings.snapshot()["snowflake"])
    assert (snowflake["queries"], snowflake["connects"]) == (threads * each, threads * each)
    # Busy time is wall time, so it never exceeds what the threads' calls summed to (each total
    # is rounded to the microsecond).
    assert 0 < snowflake["busy_s"] <= snowflake["client_s"] + snowflake["connect_s"] + 2e-6
    assert cast(dict[str, float], timings.snapshot()["phases"])["publish"] > 0


def test_every_statement_the_connector_hands_the_driver_is_one_round_trip(recording: RunTimings) -> None:
    session = FakeDriverSession(rows={}, rowcount=1)
    cursor = cast(SnowflakeCursor, session)
    session_module._execute(cursor, sql("SELECT 1"), ())
    session_module._execute(cursor, sql("SELECT 2"), ())
    assert cast(dict[str, int], recording.snapshot()["snowflake"])["queries"] == 2


def test_a_login_is_timed_as_one_connect(monkeypatch: pytest.MonkeyPatch, recording: RunTimings) -> None:
    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.session.snowflake.connector.connect",
        lambda **params: object(),
    )
    SnowflakeConnector({"account": "a"})
    assert cast(dict[str, int], recording.snapshot()["snowflake"])["connects"] == 1


def test_each_dbt_process_is_timed(tmp_path: Path, recording: RunTimings) -> None:
    subprocess_runner([sys.executable, "-c", "pass"], tmp_path)
    snapshot = recording.snapshot()
    assert snapshot["dbt_runs"] == 1
    assert cast(float, snapshot["dbt_s"]) > 0
