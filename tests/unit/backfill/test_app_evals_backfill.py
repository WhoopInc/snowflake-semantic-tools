"""Eval runs: in-order runs that all pass, progress for a run never followed, and unpaused span reads."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.poll import StatusPoll
from snowflake_semantic_tools.app.evals.progress import AttemptEnded, AttemptStarted, RunWatch
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions
from snowflake_semantic_tools.app.evals.routes import score_routing
from snowflake_semantic_tools.app.evals.run import RunEvalSuite
from snowflake_semantic_tools.domain.model.eval import EvalResultRow
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import (
    PROFILE_RUN,
    EvalSnowflake,
    compiled_eval_of,
    profile_eval_of,
    result_rows,
    spans,
    status_result,
)

OPTIONS = EvalRunOptions("abcdef0", timestamp="20260928T010203Z")


def test_fail_fast_runs_every_eval_when_each_one_is_accepted() -> None:
    port = EvalSnowflake([status_result("COMPLETED"), result_rows(), status_result("COMPLETED"), result_rows()])
    result = RunEvalSuite(port, FixedClock()).run(
        (compiled_eval_of(), compiled_eval_of()), options=OPTIONS, fail_fast=True
    )
    assert result.success
    assert [item.accepted for item in result.evals] == [True, True] and len(port.scripts) == 2


def test_a_run_that_ends_without_having_started_reports_nothing() -> None:
    events: list[object] = []
    watch = RunWatch(events.append, FixedClock(), 60_000)
    watch.ended("RUN_NEVER_STARTED", "COMPLETED")
    assert events == []
    watch.started("RUN_A", 1, 2)
    watch.ended("RUN_A", "COMPLETED")
    assert [type(item) for item in events] == [AttemptStarted, AttemptEnded]


def test_with_no_poll_interval_an_untraced_record_is_read_again_without_waiting() -> None:
    port = EvalSnowflake([spans(("r1", "DB.AGENTS.SALES")), spans(("r1", "DB.AGENTS.SALES"), ("r2",))])
    clock = FixedClock()
    options = replace(OPTIONS, poll_interval_ms=0)
    questions = compiled_questions(profile_eval_of())
    rows = tuple(
        EvalResultRow(key, query, (), index=index, record_id=record_id)
        for index, ((key, query, _), record_id) in enumerate(zip(questions, ("r1", "r2"), strict=True), start=1)
    )
    poll = StatusPoll(port, clock, options, RunWatch(lambda event: None, clock, options.heartbeat_ms))
    scored = score_routing(port, clock, options, poll, profile_eval_of(), PROFILE_RUN, rows)
    assert len(port.queries) == 2 and clock.sleeps == []
    assert scored.diagnostics == ()
