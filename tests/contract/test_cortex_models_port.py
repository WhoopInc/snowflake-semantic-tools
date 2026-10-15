"""The connector's orchestration-model probe and base-model SHOW, on a driver double."""

from __future__ import annotations

import json
from collections.abc import Mapping

import pytest
from snowflake.connector.errors import ProgrammingError

from snowflake_semantic_tools.adapters.snowflake.connector.cortex_models import (
    PROBE_REQUEST,
    PROBE_TIMEOUT_SECONDS,
    base_model,
    probe_agent_name,
    probe_statements,
    read_probe_response,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import AgentProbe, CortexBaseModel, ProbeOutcome
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession

SCOPE = SchemaScope(Identifier.parse("SALES_DB"), Identifier.parse("AGENTS"))
AGENT = "SALES_DB.AGENTS.SST_MODEL_PROBE_CLAUDE_SONNET_4_6"
ANSWERED = json.dumps({"role": "assistant", "content": [{"type": "text", "text": "OK"}]})
REFUSED = json.dumps(
    {
        "message": "llama3.1-8b is not an allowed model for Agent requests.\n\nDiagnostics:\n- Available models: x",
        "code": "399504",
    }
)


def test_the_probe_creates_runs_and_drops_one_temporary_agent_in_the_scope() -> None:
    driver = FakeDriverSession(rows={"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN": [(ANSWERED,)]})
    assert FakeDriverConnector(driver).probe_orchestration_model(SCOPE, "claude-sonnet-4-6") == AgentProbe(
        ProbeOutcome.ACCEPTED
    )
    assert driver.statements == [
        f"CREATE OR REPLACE TEMPORARY AGENT {AGENT} FROM SPECIFICATION "
        '$${"models":{"orchestration":"claude-sonnet-4-6"},"orchestration":{"budget":{"seconds":30,"tokens":2000}}}$$',
        "SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(%s, %s) AS RESPONSE",
        f"DROP AGENT IF EXISTS {AGENT}",
    ]
    assert driver.executed[1][1] == (AGENT, PROBE_REQUEST)
    # The run alone is bounded: a model that never answers would otherwise hold the session
    # until its statement timeout.
    assert driver.timeouts == [None, PROBE_TIMEOUT_SECONDS, None]


def test_a_refused_model_is_rejected_with_the_first_line_of_snowflakes_message() -> None:
    driver = FakeDriverSession(rows={"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN": [(REFUSED,)]})
    assert FakeDriverConnector(driver).probe_orchestration_model(SCOPE, "llama3.1-8b") == AgentProbe(
        ProbeOutcome.REJECTED, "llama3.1-8b is not an allowed model for Agent requests."
    )


def test_a_refused_create_runs_nothing_more_and_is_unprobed() -> None:
    denied = ProgrammingError(msg="Insufficient privileges to operate on schema 'AGENTS'", errno=3001)
    driver = FakeDriverSession(failures={"CREATE OR REPLACE TEMPORARY AGENT": denied})
    probe = FakeDriverConnector(driver).probe_orchestration_model(SCOPE, "claude-sonnet-4-6")
    assert probe.outcome is ProbeOutcome.UNPROBED
    assert "Insufficient privileges" in probe.detail
    assert len(driver.statements) == 1


def test_a_failed_run_is_unprobed_and_the_agent_is_still_dropped() -> None:
    driver = FakeDriverSession(
        failures={"SELECT SNOWFLAKE.CORTEX": ProgrammingError(msg="Unknown function", errno=2141)}
    )
    probe = FakeDriverConnector(driver).probe_orchestration_model(SCOPE, "claude-sonnet-4-6")
    assert probe.outcome is ProbeOutcome.UNPROBED
    assert driver.statements[-1] == f"DROP AGENT IF EXISTS {AGENT}"


def test_a_failed_drop_is_ignored() -> None:
    driver = FakeDriverSession(
        failures={"DROP AGENT": ProgrammingError(msg="gone", errno=2003)},
        rows={"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN": [(ANSWERED,)]},
    )
    assert FakeDriverConnector(driver).probe_orchestration_model(SCOPE, "m").outcome is ProbeOutcome.ACCEPTED


@pytest.mark.parametrize("model", ["", "claude'; DROP TABLE X; --", "a b", "-leading", "m$$"])
def test_a_model_that_is_not_a_plain_name_is_refused_before_anything_runs(model: str) -> None:
    driver = FakeDriverSession()
    with pytest.raises(SnowflakePortError, match="is not a Cortex model name"):
        FakeDriverConnector(driver).probe_orchestration_model(SCOPE, model)
    assert driver.statements == []


def test_each_model_gets_its_own_probe_agent() -> None:
    assert probe_agent_name(SCOPE, "grok-4.7").sql == "SALES_DB.AGENTS.SST_MODEL_PROBE_GROK_4_7"
    assert probe_agent_name(SCOPE, "grok-4.7") != probe_agent_name(SCOPE, "grok-4.6")
    create, _, drop = probe_statements(probe_agent_name(SCOPE, "m"), "m")
    assert str(create).startswith("CREATE OR REPLACE TEMPORARY AGENT SALES_DB.AGENTS.SST_MODEL_PROBE_M ")
    assert str(drop) == "DROP AGENT IF EXISTS SALES_DB.AGENTS.SST_MODEL_PROBE_M"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (ANSWERED, AgentProbe(ProbeOutcome.ACCEPTED)),
        ({"content": []}, AgentProbe(ProbeOutcome.ACCEPTED)),
        ('{"content": [], "code": "399504", "message": "no"}', AgentProbe(ProbeOutcome.REJECTED, "no")),
        ("not json", AgentProbe(ProbeOutcome.REJECTED, "not json")),
        (None, AgentProbe(ProbeOutcome.REJECTED, "null")),
        ({"code": "1"}, AgentProbe(ProbeOutcome.REJECTED, '{"code": "1"}')),
    ],
)
def test_the_run_response_decodes(raw: object, expected: AgentProbe) -> None:
    assert read_probe_response(raw) == expected


def test_base_models_are_read_from_show_by_folded_name() -> None:
    shown: list[Mapping[str, object]] = [
        {"name": "CLAUDE-SONNET-4-6", "lifecycle_status": "GA", "legacy_date": None, "eol_date": None},
        {"name": "CLAUDE-4-SONNET", "lifecycle_status": "EOL", "legacy_date": "2026-08-12", "eol_date": " "},
        {"name": None, "lifecycle_status": "GA"},
    ]
    driver = FakeDriverSession(rows={"SHOW CORTEX BASE MODELS IN SCHEMA SNOWFLAKE.MODELS": shown})
    assert FakeDriverConnector(driver).base_models() == (
        CortexBaseModel("claude-sonnet-4-6", "GA"),
        CortexBaseModel("claude-4-sonnet", "EOL", "2026-08-12", None),
    )
    assert driver.statements == ["SHOW CORTEX BASE MODELS IN SCHEMA SNOWFLAKE.MODELS"]


def test_a_show_row_with_no_name_is_skipped() -> None:
    assert base_model({"name": ""}) is None
