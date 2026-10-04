"""SST-VAL534: a tool's query timeout exceeds its warehouse's statement timeout."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents, observe
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _limited(limit: str) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.object_parameters[("WAREHOUSE", "WH", "STATEMENT_TIMEOUT_IN_SECONDS")] = limit
    result = compile_agents(agent("sales_agent", analyst_tool(query_timeout=600)))
    return coded(observe(port, result), "SST-VAL534")


def test_sst_val534_fires() -> None:
    [diagnostic] = _limited("300")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': query_timeout 600 exceeds STATEMENT_TIMEOUT_IN_SECONDS 300, which silently wins"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val534_silent() -> None:
    # Zero is no limit at all.
    assert _limited("3600") == _limited("0") == []
