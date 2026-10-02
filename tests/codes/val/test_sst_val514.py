"""SST-VAL514: two tools resolve to one name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents, found


def test_sst_val514_fires() -> None:
    model = agent("sales_agent", builtin_tool(), builtin_tool(description="A second chart tool."))
    [diagnostic] = found(compile_agents(model).diagnostics, "SST-VAL514")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool name 'data_to_chart' is declared twice"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val514_silent() -> None:
    model = agent("sales_agent", builtin_tool(), builtin_tool(name="charts", description="A second chart tool."))
    assert found(compile_agents(model).diagnostics, "SST-VAL514") == []
