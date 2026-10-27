"""The connector's usage reads, on a driver double: the statements, their binds, and the rows read back."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector.usage import (
    agent_usage_row,
    agent_usage_sql,
    judge_usage_row,
    judge_usage_sql,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage, JudgeUsage
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession

STARTED = datetime(2026, 10, 22, 18, 3, tzinfo=UTC)


def test_agent_usage_binds_the_start_and_every_request_as_one_array_and_reads_each_row() -> None:
    driver = FakeDriverSession(
        rows={"SELECT REQUEST_ID": [("r1", Decimal("0.25"), None, STARTED), ("r2", "0.5", 0.01, "2026-10-22")]}
    )
    found = FakeDriverConnector(driver).agent_usage(["r2", "r1"], since="2026-10-22T17:00:00+00:00")
    assert found == (
        AgentUsage("r1", 0.25, None, "2026-10-22T18:03:00+00:00"),
        AgentUsage("r2", 0.5, 0.01, "2026-10-22"),
    )
    [(statement, binds)] = driver.executed
    assert statement == str(agent_usage_sql())
    assert binds == ("2026-10-22T17:00:00+00:00", json.dumps(["r1", "r2"]))
    assert "CORTEX_AGENT_USAGE_HISTORY" in statement and "METADATA:sql_query_credits::FLOAT" in statement


def test_judge_usage_binds_the_run_names_and_reads_each_runs_counts() -> None:
    driver = FakeDriverSession(rows={"WITH SPANS": [("RUN_1", 4, Decimal(3), Decimal(900))]})
    assert FakeDriverConnector(driver).judge_usage(["RUN_1"]) == (JudgeUsage("RUN_1", 4, 3, 900),)
    [(statement, binds)] = driver.executed
    assert statement == str(judge_usage_sql()) and binds == (json.dumps(["RUN_1"]),)
    for part in ("SPLIT_TO_TABLE", "ai.observability.eval.inference_request_ids", "CORTEX_REST_API_USAGE_HISTORY"):
        assert part in statement


def test_nothing_to_read_sends_nothing() -> None:
    driver = FakeDriverSession()
    connector = FakeDriverConnector(driver)
    assert connector.agent_usage([], since="now") == () and connector.judge_usage([]) == ()
    assert driver.executed == []


def test_a_row_without_its_key_or_with_a_value_that_is_no_number_is_refused() -> None:
    with pytest.raises(SnowflakePortError, match="no REQUEST_ID"):
        agent_usage_row((None, 1, None, None))
    with pytest.raises(SnowflakePortError, match="TOKEN_CREDITS is 'x', not a number"):
        agent_usage_row(("r1", "x"))
    with pytest.raises(SnowflakePortError, match="no RUN_NAME"):
        judge_usage_row(())
    assert judge_usage_row(("RUN", None, None, None)) == JudgeUsage("RUN", 0, 0, 0)
    assert agent_usage_row(("r1", None)) == AgentUsage("r1", 0.0, None, "")
