"""SST-VAL531: an object a tool calls, which SST does not publish, does not exist."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, observe, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _called(existing: set[str]) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.existing = existing
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    return coded(observe(port, result), "SST-VAL531")


def test_sst_val531_fires() -> None:
    [diagnostic] = _called(set())
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': 'DB.DEV.LOOKUP' does not exist in target 'dev'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val531_silent() -> None:
    assert _called({"DB.DEV.LOOKUP"}) == []
