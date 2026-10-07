"""Polling an eval run through status reads that fail in transit, against a deadline, with progress.

Every run here is the fake's scripted session on a `PollClock`, whose sleeps pass, so a
deadline and a heartbeat interval are reached without waiting on the wall clock.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TypeVar

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.progress import (
    AttemptEnded,
    AttemptStarted,
    EvalProgressEvent,
    StatusChanged,
    StatusReadFailed,
    StillWaiting,
    duration_text,
    short_reason,
)
from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, RunEvalSuite
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError, SnowflakeTransientError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.clocks import PollClock
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of, result_rows, status_result

RUN = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"
DROPPED = SnowflakeTransientError("250003: Failed to execute request: Read timed out.\nretry 4 of 4")
EventT = TypeVar("EventT")


class SlowReads(EvalSnowflake):
    """Each status read takes `read_ms` of the clock's time, as a read over a failing network does."""

    def __init__(self, results: list[QueryResult | Exception], clock: PollClock, read_ms: int) -> None:
        super().__init__(results)
        self.clock = clock
        self.read_ms = read_ms

    def query_in_context(
        self, scope: SchemaScope, sql: Sql, params: object = None, *, timeout_seconds: int | None = None
    ) -> QueryResult:
        if "'STATUS'" in str(sql):
            self.clock.advance(self.read_ms)
        return super().query_in_context(scope, sql, params, timeout_seconds=timeout_seconds)


def retrying_eval() -> CompiledEval:
    """The eval with one retry allowed, so a test can show none was needed."""
    compiled = compiled_eval_of()
    run = compiled.resolved.config.run
    assert run is not None
    config = replace(compiled.resolved.config, run=replace(run, retry=1))
    return replace(compiled, resolved=replace(compiled.resolved, config=config))


def run_polled(
    port: EvalSnowflake,
    *,
    clock: PollClock | None = None,
    compiled: CompiledEval | None = None,
    **options: int,
) -> tuple[EvalSuiteResult, list[EvalProgressEvent]]:
    events: list[EvalProgressEvent] = []
    result = RunEvalSuite(port, clock or PollClock(), progress=events.append).run(
        (compiled or compiled_eval_of(),),
        options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z", **options),
    )
    return result, events


def status_reads(port: EvalSnowflake) -> int:
    return sum("'STATUS'" in query for query, _ in port.queries)


def starts(port: EvalSnowflake) -> int:
    return sum("'START'" in statement for script in port.scripts for statement in script)


def of_kind(events: list[EvalProgressEvent], kind: type[EventT]) -> list[EventT]:
    return [event for event in events if isinstance(event, kind)]


def test_a_read_that_fails_in_transit_is_retried_and_the_completed_run_is_kept_without_a_retry() -> None:
    port = EvalSnowflake([status_result("CREATED"), DROPPED, status_result("COMPLETED"), result_rows()])

    result, events = run_polled(port, compiled=retrying_eval())

    assert result.success and not result.diagnostics
    [attempt] = result.evals[0].attempts
    assert attempt.terminal_status == "COMPLETED"
    assert (starts(port), status_reads(port)) == (1, 3)
    assert of_kind(events, StatusReadFailed) == [
        StatusReadFailed(RUN, 1, 5, "250003: Failed to execute request: Read timed out.")
    ]


def test_max_read_failures_in_a_row_then_a_failed_last_read_fail_the_attempt() -> None:
    port = EvalSnowflake([DROPPED] * 6)

    result, events = run_polled(port)

    [attempt] = result.evals[0].attempts
    assert attempt.terminal_status == "STATUS_FAILED"
    assert attempt.retrieval_error == (
        f"evaluation run '{RUN}': its status could not be read in 6 reads in a row "
        "(250003: Failed to execute request: Read timed out.)"
    )
    [diagnostic] = result.diagnostics
    assert diagnostic.code == "SST-APL023"
    # The last read is made, and fails, without a warning that it will be retried.
    assert status_reads(port) == 6
    assert [event.failures for event in of_kind(events, StatusReadFailed)] == [1, 2, 3, 4, 5]
    [ended] = of_kind(events, AttemptEnded)
    assert ended == events[-1] and ended.status == "STATUS_FAILED"


def test_a_success_resets_the_failures_in_a_row() -> None:
    dropped: list[QueryResult | Exception] = [DROPPED] * 4
    port = EvalSnowflake([*dropped, status_result("CREATED"), *dropped, status_result("COMPLETED"), result_rows()])

    result, _ = run_polled(port)

    assert result.success
    assert status_reads(port) == 10


def test_the_last_read_after_the_failures_ran_out_decides_when_the_run_ended() -> None:
    port = EvalSnowflake([*[DROPPED] * 5, status_result("COMPLETED"), result_rows()])

    result, _ = run_polled(port)

    assert result.success
    assert status_reads(port) == 6


def test_a_last_read_that_finds_the_run_still_going_with_time_left_goes_on_polling() -> None:
    port = EvalSnowflake([*[DROPPED] * 5, status_result("CREATED"), status_result("COMPLETED"), result_rows()])

    result, _ = run_polled(port)

    assert result.success
    assert status_reads(port) == 7


def test_the_read_at_the_deadline_decides_after_a_failed_read() -> None:
    port = EvalSnowflake([status_result("CREATED"), DROPPED, status_result("COMPLETED"), result_rows()])

    result, _ = run_polled(port, deadline_ms=10_000)

    assert result.success
    assert status_reads(port) == 3


def test_the_deadline_is_the_clocks_however_slow_the_reads_are() -> None:
    clock = PollClock()
    port = SlowReads([status_result("CREATED")] * 3, clock, read_ms=40_000)

    result, _ = run_polled(port, clock=clock, deadline_ms=60_000, read_timeout_ms=30_000)

    [attempt] = result.evals[0].attempts
    assert attempt.terminal_status == "STATUS_FAILED"
    assert "did not reach a terminal status within 1m00s; it was CREATED" in (attempt.retrieval_error or "")
    # Each read is given no more than what is left of the deadline; the one made once it has
    # passed is the last, given the whole read timeout. No wait runs past the deadline.
    assert [sent.timeout_seconds for sent in port.log if sent.kind == "query"] == [30, 15, 30]
    assert clock.sleeps == [5_000]


def test_a_status_is_reported_once_per_change_with_the_attempts_start_and_end() -> None:
    statuses = ("CREATED", "CREATED", "INVOCATION_IN_PROGRESS", "INVOCATION_IN_PROGRESS", "COMPLETED")
    port = EvalSnowflake([*(status_result(status) for status in statuses), result_rows()])

    _, events = run_polled(port, compiled=retrying_eval())

    assert events[0] == AttemptStarted(RUN, 1, 2)
    assert [event.status for event in of_kind(events, StatusChanged)] == [
        "CREATED",
        "INVOCATION_IN_PROGRESS",
        "COMPLETED",
    ]
    [ended] = of_kind(events, AttemptEnded)
    assert ended == events[-1] and ended.status == "COMPLETED"
    assert ended.line.startswith(f"eval run {RUN}: ended COMPLETED after 20s")


def test_the_heartbeat_names_the_runs_in_flight_at_most_once_an_interval() -> None:
    port = EvalSnowflake([*[status_result("COMPUTATION_IN_PROGRESS")] * 30, status_result("COMPLETED"), result_rows()])

    _, events = run_polled(port)

    beats = of_kind(events, StillWaiting)
    assert len(beats) == 2
    assert [beat.line for beat in beats] == [
        f"waiting on 1 run: {RUN} COMPUTATION_IN_PROGRESS 1m00s",
        f"waiting on 1 run: {RUN} COMPUTATION_IN_PROGRESS 2m00s",
    ]


@pytest.mark.parametrize(
    "answer",
    [
        status_result("COMPLETED", run_name="ANOTHER_RUN"),
        SnowflakePortError("002003 (02000): SQL compilation error: Unknown function EXECUTE_AI_EVALUATION"),
        QueryResult(("RUN_NAME",), ((RUN,),)),
    ],
    ids=["another-run", "refused", "malformed"],
)
def test_a_read_snowflake_answered_unusably_fails_the_attempt_at_once(answer: QueryResult | Exception) -> None:
    port = EvalSnowflake([answer])

    result, events = run_polled(port)

    assert result.evals[0].attempts[0].terminal_status == "STATUS_FAILED"
    assert status_reads(port) == 1
    assert not of_kind(events, StatusReadFailed)


def test_progress_lines_render_durations_and_short_reasons() -> None:
    assert [duration_text(ms) for ms in (-5, 45_000, 250_000, 3_720_000)] == ["0s", "45s", "4m10s", "1h02m"]
    assert short_reason("") == "no reason given"
    assert short_reason("x" * 200) == "x" * 117 + "..."
    assert StillWaiting(((f"{RUN}_R3", "COMPUTATION_IN_PROGRESS", 250_000), ("R4", "STARTING", 0))).line == (
        f"waiting on 2 runs: {RUN}_R3 COMPUTATION_IN_PROGRESS 4m10s, R4 STARTING 0s"
    )
    assert StatusReadFailed(RUN, 1, 5, "Read timed out.").line == (
        f"eval run {RUN}: status read failed (1/5), retrying: Read timed out."
    )
