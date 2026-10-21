"""Reading a CoWork profile run's router spans, and scoring `routing_accuracy` from them.

CoWork profiles are a Snowflake private preview. One query reads a run's root and routing
spans; a record with no root span is read again within the attempt's deadline, and one still
untraced fails closed. The suite merges the routing results into each attempt's rows.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.poll import StatusPoll
from snowflake_semantic_tools.app.evals.progress import RunWatch, no_progress
from snowflake_semantic_tools.app.evals.projection import _question_json, eval_suite_json
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions
from snowflake_semantic_tools.app.evals.routes import ROUTE_READS, RoutingScored, read_routes, score_routing
from snowflake_semantic_tools.app.evals.run import RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalResultRow
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import (
    SnowflakePortError,
    SnowflakeStatementTimedOut,
    SnowflakeTransientError,
)
from tests.helpers.clocks import FixedClock, PollClock
from tests.helpers.eval_builders import (
    PROFILE_RUN,
    ROUTE_COLUMNS,
    EvalSnowflake,
    compile_profile_eval,
    profile_eval_of,
    profile_results,
    profile_status,
    spans,
)
from tests.helpers.eval_inputs import concierge_eval, settled

SALES = "DB.AGENTS.SALES"
PARTNER = "PARTNER.AGENTS.DELIVERY"
OPTIONS = EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
ROUTES_SQL = (
    'SELECT record_attributes:"ai.observability.record_id"::string AS RECORD_ID,'
    ' record_attributes:"ai.observability.span_type"::string AS SPAN_TYPE,'
    ' record_attributes:"snow.ai.observability.agent.tool.agent_router.subagent_name"::string AS SUBAGENT'
    " FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS"
    ' WHERE record_attributes:"snow.ai.observability.run.name"::string = %s'
    ' AND record_attributes:"snow.ai.observability.database.name"::string = %s'
    ' AND record_attributes:"snow.ai.observability.schema.name"::string = %s'
    ' AND record_attributes:"snow.ai.observability.object.name"::string = %s'
    " AND (record_attributes:\"ai.observability.span_type\"::string = 'record_root'"
    ' OR record_attributes:"snow.ai.observability.agent.tool.agent_router.subagent_name" IS NOT NULL)'
)


def _rows(*record_ids: str | None) -> tuple[EvalResultRow, ...]:
    """The rows `read_results` gives the default profile eval, with the record ids given."""
    questions = compiled_questions(profile_eval_of())
    return tuple(
        EvalResultRow(key, query, (), index=index, record_id=record_id)
        for index, ((key, query, _), record_id) in enumerate(zip(questions, record_ids, strict=True), start=1)
    )


def _score(
    port: EvalSnowflake, rows: tuple[EvalResultRow, ...], clock: FixedClock, options: EvalRunOptions = OPTIONS
) -> RoutingScored:
    poll = StatusPoll(port, clock, options, RunWatch(no_progress, clock, options.heartbeat_ms))
    return score_routing(port, clock, options, poll, profile_eval_of(), PROFILE_RUN, rows)


def _routing(row: EvalResultRow) -> tuple[float | None, bool, str | None]:
    [metric] = [metric for metric in row.metrics if metric.metric_name == "routing_accuracy"]
    return metric.score, metric.passed, metric.explanation


def test_one_query_reads_the_runs_roots_and_routes_for_the_profile() -> None:
    port = EvalSnowflake([spans(("r1", SALES), ("r2",), ("r3", PARTNER, SALES, PARTNER))])

    found = read_routes(port, profile_eval_of(), PROFILE_RUN, timeout_seconds=7)

    assert port.queries == [(ROUTES_SQL, (PROFILE_RUN, "DB", "PROFILES", "CONCIERGE"))]
    assert port.log[0].scope == "DB.PROFILES" and port.log[0].timeout_seconds == 7
    assert found.records_with_root == {"r1", "r2", "r3"}
    # Each record's routes are sorted, repeats kept; a traced record with no route declined.
    assert (found.routed("r1"), found.routed("r2"), found.routed("r3")) == (
        (SALES,),
        (),
        (SALES, PARTNER, PARTNER),
    )
    assert found.routed("untraced") is None


def test_the_profile_is_bound_as_snowflake_resolves_its_name() -> None:
    [compiled] = compile_profile_eval(concierge_eval(), QualifiedName.parse('db.profiles."Concierge"')).compiled
    assert isinstance(compiled, CompiledEval)
    port = EvalSnowflake([spans()])

    read_routes(port, compiled, PROFILE_RUN)

    assert port.queries[0][1] == (PROFILE_RUN, "DB", "PROFILES", "Concierge")


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        (QueryResult(("RECORD_ID", "SUBAGENT"), ()), "router spans omitted columns: SPAN_TYPE"),
        (QueryResult(ROUTE_COLUMNS, ((" ", "record_root", None),)), "a router span names no record"),
    ],
)
def test_spans_sst_cannot_read_are_refused(answer: QueryResult, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        read_routes(EvalSnowflake([answer]), profile_eval_of(), PROFILE_RUN)


def test_every_row_gains_its_routing_result_and_its_expected_and_routed_subagents() -> None:
    port = EvalSnowflake([spans(("r1", SALES), ("r2", PARTNER))])

    scored = _score(port, _rows("r1", "r2"), FixedClock())

    first, second = scored.rows
    assert scored.diagnostics == ()
    assert (first.expected_subagents, first.routed_subagents) == ((SALES,), (SALES,))
    assert _routing(first) == (1.0, True, f"expected {SALES}; the router chose {SALES}")
    assert (second.expected_subagents, second.routed_subagents) == ((), (PARTNER,))
    assert _routing(second) == (0.0, False, f"expected a decline; the router chose {PARTNER}")


def test_an_untraced_record_is_read_again_the_poll_interval_apart_until_it_is_traced() -> None:
    port = EvalSnowflake([spans(("r1", SALES)), spans(("r1", SALES), ("r2",))])
    clock = FixedClock()

    scored = _score(port, _rows("r1", "r2"), clock)

    assert len(port.queries) == 2 and clock.sleeps == [OPTIONS.poll_interval_ms]
    assert scored.diagnostics == ()
    assert _routing(scored.rows[1]) == (1.0, True, "expected a decline; the router chose no subagent")


def test_a_record_still_untraced_after_the_last_read_fails_closed_with_a_warning() -> None:
    port = EvalSnowflake([spans(("r1", SALES)) for _ in range(ROUTE_READS)])
    clock = FixedClock()

    scored = _score(port, _rows("r1", "r2"), clock)

    assert len(port.queries) == ROUTE_READS and len(clock.sleeps) == ROUTE_READS - 1
    [warning] = scored.diagnostics
    assert (warning.code, warning.severity) == ("SST-SNO035", Severity.WARNING)
    assert scored.rows[1].routed_subagents is None
    assert _routing(scored.rows[1])[:2] == (None, False)


def test_a_row_with_no_record_id_is_untraced() -> None:
    port = EvalSnowflake([spans(("r2",)) for _ in range(ROUTE_READS)])

    scored = _score(port, _rows(None, "r2"), FixedClock())

    assert [row.routed_subagents for row in scored.rows] == [None, ()]
    assert [item.code for item in scored.diagnostics] == ["SST-SNO035"]


def test_no_read_waits_past_the_attempts_deadline() -> None:
    clock = PollClock()
    options = replace(OPTIONS, deadline_ms=45, poll_interval_ms=40)
    port = EvalSnowflake([spans(("r1", SALES)), spans(("r1", SALES))])
    poll = StatusPoll(port, clock, options, RunWatch(no_progress, clock, options.heartbeat_ms))

    scored = score_routing(port, clock, options, poll, profile_eval_of(), PROFILE_RUN, _rows("r1", "r2"))

    # The first wait is the poll interval; the second read finds the deadline passed.
    assert clock.sleeps == [40] and len(port.queries) == 2
    assert [item.code for item in scored.diagnostics] == ["SST-SNO035"]


def test_a_read_that_fails_in_transit_is_retried() -> None:
    port = EvalSnowflake(
        [SnowflakeStatementTimedOut("slow"), SnowflakeTransientError("dropped"), spans(("r1",), ("r2",))]
    )

    scored = _score(port, _rows("r1", "r2"), FixedClock())

    assert len(port.queries) == 3 and scored.diagnostics == ()


def test_reads_that_keep_failing_in_transit_raise_the_last_failure() -> None:
    port = EvalSnowflake([SnowflakeTransientError(f"dropped {n}") for n in range(ROUTE_READS)])

    with pytest.raises(SnowflakeTransientError, match=f"dropped {ROUTE_READS - 1}"):
        _score(port, _rows("r1", "r2"), FixedClock())


def test_a_last_read_failing_in_transit_keeps_what_an_earlier_read_found() -> None:
    port = EvalSnowflake([spans(("r1", SALES)), *(SnowflakeTransientError("dropped") for _ in range(ROUTE_READS - 1))])

    scored = _score(port, _rows("r1", "r2"), FixedClock())

    assert [row.routed_subagents for row in scored.rows] == [(SALES,), None]
    assert [item.code for item in scored.diagnostics] == ["SST-SNO035"]


def test_a_refused_read_raises_at_once() -> None:
    port = EvalSnowflake([SnowflakePortError("Insufficient privileges")])

    with pytest.raises(SnowflakePortError, match="Insufficient privileges"):
        _score(port, _rows("r1", "r2"), FixedClock())
    assert len(port.queries) == 1


def test_a_profile_run_merges_its_routing_into_each_question() -> None:
    compiled = profile_eval_of()
    port = EvalSnowflake([profile_status("COMPLETED"), profile_results(compiled), spans(("r1", SALES), ("r2",))])

    result = RunEvalSuite(port, FixedClock()).run((compiled,), options=OPTIONS)

    assert result.success and not result.diagnostics
    [attempt] = result.evals[0].attempts
    assert [row.record_id for row in attempt.rows] == ["r1", "r2"]
    assert [[metric.metric_name for metric in row.metrics] for row in attempt.rows] == [
        ["answer_correctness", "routing_accuracy"],
        ["answer_correctness", "routing_accuracy"],
    ]
    assert [_routing(row)[:2] for row in attempt.rows] == [(1.0, True), (1.0, True)]
    assert port.queries[-1] == (ROUTES_SQL, (attempt.run_name, "DB", "PROFILES", "CONCIERGE"))


def test_a_profile_run_whose_spans_cannot_be_read_is_not_accepted() -> None:
    compiled = profile_eval_of()
    compiled = replace(compiled, resolved=settled(compiled.resolved, retry=0))
    refused = SnowflakePortError("Insufficient privileges to operate on table 'AI_OBSERVABILITY_EVENTS'")
    port = EvalSnowflake([profile_status("COMPLETED"), profile_results(compiled), refused])

    result = RunEvalSuite(port, FixedClock()).run((compiled,), options=OPTIONS)

    assert not result.success and not result.evals[0].accepted
    [attempt] = result.evals[0].attempts
    assert attempt.rows == () and attempt.retrieval_error == str(refused)
    assert [(item.code, item.severity) for item in result.diagnostics] == [("SST-SNO034", Severity.ERROR)]


def test_a_profile_eval_with_no_routing_metric_reads_no_spans() -> None:
    compiled = profile_eval_of(concierge_eval(routing_metrics=()))
    port = EvalSnowflake([profile_status("COMPLETED"), profile_results(compiled)])

    result = RunEvalSuite(port, FixedClock()).run((compiled,), options=OPTIONS)

    assert result.success and not port.query_results
    assert all(row.routed_subagents is None for row in result.evals[0].attempts[0].rows)


def test_the_json_shows_a_profile_questions_subagents_and_an_agents_none() -> None:
    compiled = profile_eval_of()
    port = EvalSnowflake([profile_status("COMPLETED"), profile_results(compiled), spans(("r1", PARTNER), ("r2",))])
    result = RunEvalSuite(port, FixedClock()).run((compiled,), options=OPTIONS)

    attempt = eval_suite_json(result)["evals"][0]["attempts"][0]  # type: ignore[index]
    first = attempt["questions"][0]
    assert {
        key: first[key] for key in ("expected_tools", "called_tools", "expected_subagents", "routed_subagents")
    } == {
        "expected_tools": None,
        "called_tools": None,
        "expected_subagents": [SALES],
        "routed_subagents": [PARTNER],
    }
    assert first["metrics"][1] == {
        "metric_name": "routing_accuracy",
        "score": 0.0,
        "passed": False,
        "threshold": {"min": 0.9, "max": None},
        "explanation": f"expected {SALES}; the router chose {PARTNER}",
    }
    assert first["passed"] is False and attempt["questions"][1]["passed"] is True
    assert attempt["metric_summaries"][1] == {
        "metric_name": "routing_accuracy",
        "record_count": 2,
        "passed_count": 1,
        "average_score": 0.5,
    }
    agent_row = EvalResultRow("q", "Question", ())
    assert "expected_subagents" not in _question_json(agent_row, frozenset())
