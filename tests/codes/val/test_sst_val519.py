"""SST-VAL519: two tool descriptions are near-duplicates."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents, found


def test_sst_val519_fires() -> None:
    model = agent(
        "sales_agent",
        builtin_tool(description="Draws a chart of the rows another tool returned."),
        builtin_tool("code_execution", description="Draws a chart of the rows another tool returns."),
    )
    [diagnostic] = found(compile_agents(model).diagnostics, "SST-VAL519")
    assert diagnostic.severity is Severity.WARNING
    assert (
        diagnostic.message
        == "agent 'sales_agent': 'data_to_chart' and 'code_execution' have near-identical descriptions"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val519_silent() -> None:
    model = agent(
        "sales_agent",
        builtin_tool(description="Draws a chart of the rows another tool returned."),
        builtin_tool("code_execution", description="Runs Python for arithmetic the views cannot express."),
    )
    assert found(compile_agents(model).diagnostics, "SST-VAL519") == []
