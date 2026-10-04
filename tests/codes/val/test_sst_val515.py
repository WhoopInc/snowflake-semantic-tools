"""SST-VAL515: two tool names differ only by case."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val515_fires() -> None:
    model = agent("sales_agent", builtin_tool(name="charts"), builtin_tool(name="Charts", description="Other charts."))
    [diagnostic] = coded(compile_agents(model).diagnostics, "SST-VAL515")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': 'charts' and 'Charts' differ only by case"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val515_silent() -> None:
    model = agent("sales_agent", builtin_tool(name="charts"), builtin_tool(name="graphs", description="Other charts."))
    assert coded(compile_agents(model).diagnostics, "SST-VAL515") == []
