"""The connector's inline agent run: a temporary agent created, run once, and dropped, on a driver double."""

from __future__ import annotations

import json

import pytest
from snowflake.connector.errors import ProgrammingError

from snowflake_semantic_tools.adapters.snowflake.connector.agent_run import (
    ask_agent_name,
    ask_request,
    ask_statements,
    read_agent_reply,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolResult, ToolUse
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession

SCOPE = SchemaScope(Identifier.parse("SALES_DB"), Identifier.parse("AGENTS"))
SPEC = '{"models":{"orchestration":"claude-sonnet-4-6"},"tools":[]}\n'
# The shape measured on a live account: Snowflake's own system tools beside the declared one.
REPLY = json.dumps(
    {
        "role": "assistant",
        "content": [
            {"type": "thinking", "thinking": {"text": "Plan."}},
            {
                "type": "tool_use",
                "tool_use": {
                    "name": "SALES",
                    "type": "cortex_analyst_text_to_sql",
                    "input": {"query": "orders by month"},
                    "tool_use_id": "t1",
                    "client_side_execute": False,
                },
            },
            {
                "type": "tool_result",
                "tool_result": {
                    "name": "SALES",
                    "status": "success",
                    "type": "cortex_analyst_text_to_sql",
                    "tool_use_id": "t1",
                    "content": [{"type": "json", "json": {"sql": "SELECT 1"}}],
                },
            },
            {"type": "tool_use", "tool_use": {"name": "system_execute_sql", "type": "system", "input": "x"}},
            {"type": "text", "text": "There were 12 orders."},
            {"type": "suggested_queries", "suggested_queries": []},
            "not an item",
        ],
    }
)


def test_a_run_creates_runs_and_drops_one_temporary_agent_and_reads_its_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.agent_run.secrets.token_hex", lambda n: "ab12cd34"
    )
    driver = FakeDriverSession(rows={"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN": [(REPLY,)]})
    reply = FakeDriverConnector(driver).run_inline(SCOPE, "sales-agent", SPEC, "How many orders?", 90)
    agent = "SALES_DB.AGENTS.SST_ASK_SALES_AGENT_AB12CD34"
    assert driver.statements == [
        f"CREATE OR REPLACE TEMPORARY AGENT {agent} FROM SPECIFICATION "
        '$${"models":{"orchestration":"claude-sonnet-4-6"},"tools":[]}$$',
        "SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(%s, %s) AS RESPONSE",
        f"DROP AGENT IF EXISTS {agent}",
    ]
    assert driver.executed[1][1] == (agent, ask_request("How many orders?"))
    # The run alone is bounded, by the caller's timeout.
    assert driver.timeouts == [None, 90, None]
    assert reply.answer == "There were 12 orders."
    assert reply.tool_uses == (
        ToolUse("SALES", "cortex_analyst_text_to_sql", {"query": "orders by month"}),
        ToolUse("system_execute_sql", "system", {}),
    )
    assert reply.tool_results == (ToolResult("SALES", "success"),)
    assert reply.error is None and reply.duration_ms >= 0


def test_the_request_carries_the_question_as_one_user_message() -> None:
    assert json.loads(ask_request("Why?")) == {
        "messages": [{"role": "user", "content": [{"type": "text", "text": "Why?"}]}]
    }


def test_each_run_gets_its_own_temporary_agent_named_for_the_agent() -> None:
    assert ask_agent_name(SCOPE, "sales", "0f0f0f0f").sql == "SALES_DB.AGENTS.SST_ASK_SALES_0F0F0F0F"
    assert ask_agent_name(SCOPE, "sales", "0f0f0f0f") != ask_agent_name(SCOPE, "sales", "1e1e1e1e")


def test_a_specification_holding_a_dollar_quote_is_refused_before_anything_runs() -> None:
    driver = FakeDriverSession()
    with pytest.raises(ValueError, match=r"\$\$"):
        FakeDriverConnector(driver).run_inline(SCOPE, "sales", '{"x":"$$"}', "q", 10)
    assert driver.statements == []
    with pytest.raises(ValueError):
        ask_statements(ask_agent_name(SCOPE, "s", "00000000"), "$$")


def test_a_refused_create_raises_and_the_drop_is_still_attempted() -> None:
    denied = ProgrammingError(msg="Insufficient privileges to operate on schema 'AGENTS'", errno=3001)
    driver = FakeDriverSession(failures={"CREATE OR REPLACE TEMPORARY AGENT": denied})
    with pytest.raises(SnowflakePortError, match="Insufficient privileges"):
        FakeDriverConnector(driver).run_inline(SCOPE, "sales", SPEC, "q", 10)
    assert [statement.split()[0] for statement in driver.statements] == ["CREATE", "DROP"]


def test_a_failed_run_raises_and_the_agent_is_still_dropped() -> None:
    driver = FakeDriverSession(failures={"SELECT SNOWFLAKE.CORTEX": ProgrammingError(msg="timed out", errno=604)})
    with pytest.raises(SnowflakePortError):
        FakeDriverConnector(driver).run_inline(SCOPE, "sales", SPEC, "q", 10)
    assert driver.statements[-1].startswith("DROP AGENT IF EXISTS SALES_DB.AGENTS.SST_ASK_SALES_")


def test_a_failed_drop_is_ignored() -> None:
    driver = FakeDriverSession(
        failures={"DROP AGENT": ProgrammingError(msg="gone", errno=2003)},
        rows={"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN": [(REPLY,)]},
    )
    assert FakeDriverConnector(driver).run_inline(SCOPE, "sales", SPEC, "q", 10).answer == "There were 12 orders."


def test_a_run_with_no_row_is_an_error_reply() -> None:
    driver = FakeDriverSession(rows={"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN": []})
    assert FakeDriverConnector(driver).run_inline(SCOPE, "sales", SPEC, "q", 10).error == "null"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (REPLY, None),
        ('{"code": "399513", "message": "Agent does not exist.\\nmore"}', "Agent does not exist."),
        ('{"content": [], "code": "399504", "message": "no"}', "no"),
        ('{"message": ""}', '{"message": ""}'),
        ("not json", "not json"),
        ("", "the agent returned no response"),
        (None, "null"),
        ([1], "[1]"),
    ],
)
def test_the_run_response_decodes_to_a_reply_or_its_error(raw: object, expected: str | None) -> None:
    assert read_agent_reply(raw).error == expected


def test_an_item_without_its_body_reads_as_empty() -> None:
    raw = {"content": [{"type": "tool_use"}, {"type": "tool_result", "tool_result": "x"}, {"type": "text"}]}
    assert read_agent_reply(raw, 7) == AgentReply("", (ToolUse("", ""),), (ToolResult("", ""),), None, 7)


def test_the_run_response_reports_its_tokens_when_its_usage_does() -> None:
    usage = {"tokens_consumed": [{"input_tokens": {"total": 120}, "output_tokens": {"total": 30}}]}
    counted = {"content": [{"type": "text", "text": "Hi"}], "metadata": {"usage": usage}}
    assert read_agent_reply(json.dumps(counted)).tokens == (120, 30)
    assert read_agent_reply({"content": [], "metadata": "none"}).tokens is None
    assert read_agent_reply({"content": []}).tokens is None
