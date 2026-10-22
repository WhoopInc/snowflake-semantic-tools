"""Reading a comparison's cost: each amount known, pending, or unavailable, from two usage reads."""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.compare_cost import CostQuery, read_costs
from snowflake_semantic_tools.domain.evaluate.compare import KNOWN, PENDING, UNAVAILABLE, Amount
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage, JudgeUsage
from tests.helpers.snowflake_fake import FakeSnowflake

QUERY = CostQuery("eval:sales", "opus", ("r1", "r2"), ("RUN_1",))
STARTED = "2026-10-22T14:00:00Z"


def landed(port: FakeSnowflake, *, sql: float | None = 0.25, started: str = "2026-10-22T14:01:00+00:00") -> None:
    for request in QUERY.request_ids:
        port.agent_usage_rows[request] = AgentUsage(request, 0.5, sql, started)
    port.judge_usage_rows["RUN_1"] = JudgeUsage("RUN_1", 3, 3, 900)


def test_usage_that_has_landed_is_known_and_is_read_in_two_statements() -> None:
    port = FakeSnowflake()
    landed(port)
    cost = read_costs(port, (QUERY,), started_at=STARTED, now="2026-10-22T15:00:00Z")[("eval:sales", "opus")]
    assert (cost.agent_credits, cost.sql_credits, cost.judge_tokens) == (
        Amount(KNOWN, 1.0),
        Amount(KNOWN, 0.5),
        Amount(KNOWN, 900.0),
    )
    # The agent read looks from an hour before the comparison started, for clock skew.
    assert port.queries == [("agent_usage", ("2026-10-22T13:00:00+00:00", ("r1", "r2"))), ("judge_usage", ("RUN_1",))]


def test_a_null_sql_credit_is_pending_until_eight_hours_have_passed() -> None:
    port = FakeSnowflake()
    landed(port, sql=None)
    soon = read_costs(port, (QUERY,), started_at=STARTED, now="2026-10-22T15:00:00Z")[("eval:sales", "opus")]
    later = read_costs(port, (QUERY,), started_at=STARTED, now="2026-10-23T00:00:00Z")[("eval:sales", "opus")]
    assert (soon.sql_credits.state, later.sql_credits) == (PENDING, Amount(KNOWN, 0.0))
    # A time that does not parse never counts as settled.
    unknown = read_costs(port, (QUERY,), started_at="soon", now="later")[("eval:sales", "opus")]
    assert unknown.sql_credits.state == PENDING


def test_a_request_or_a_judge_call_that_has_not_landed_is_pending() -> None:
    port = FakeSnowflake()
    port.agent_usage_rows["r1"] = AgentUsage("r1", 0.5, 0.0, "2026-10-22T14:01:00+00:00")
    port.judge_usage_rows["RUN_1"] = JudgeUsage("RUN_1", 3, 2, 600)
    cost = read_costs(port, (QUERY,), started_at=STARTED, now=STARTED)[("eval:sales", "opus")]
    assert {cost.agent_credits.state, cost.sql_credits.state, cost.judge_tokens.state} == {PENDING}


def test_a_refused_read_or_nothing_to_read_is_unavailable_with_why() -> None:
    port = FakeSnowflake()
    port.fail("agent_usage", SnowflakePortError("Insufficient privileges\nto read"))
    port.fail("judge_usage", SnowflakePortError(""))
    cost = read_costs(port, (QUERY,), started_at=STARTED, now=STARTED)[("eval:sales", "opus")]
    assert cost.agent_credits == Amount(UNAVAILABLE, reason="Insufficient privileges")
    assert cost.judge_tokens == Amount(UNAVAILABLE, reason="the usage view could not be read")
    empty = read_costs(
        FakeSnowflake(), (CostQuery("eval:sales", "opus", (), ("RUN_9",)),), started_at=STARTED, now=STARTED
    )
    nothing = empty[("eval:sales", "opus")]
    assert nothing.agent_credits.reason == "its runs' results named no agent request"
    assert nothing.judge_tokens.reason == "no eval span of its runs names a judge request"
