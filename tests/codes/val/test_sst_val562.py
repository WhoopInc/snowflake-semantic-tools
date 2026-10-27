"""SST-VAL562: a data_to_chart tool is declared, which an agent does not need to chart."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val562_fires() -> None:
    [diagnostic] = coded(compile_agents(agent("sales_agent", builtin_tool("data_to_chart"))).diagnostics, "SST-VAL562")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "agent 'sales_agent': tool 'data_to_chart' of type data_to_chart is not needed; the agent charts without it"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_a_declared_data_to_chart_tool_is_still_rendered() -> None:
    result = compile_agents(agent("sales_agent", builtin_tool("data_to_chart")))
    [compiled] = result.compiled
    assert "data_to_chart" in str(compiled)


def test_sst_val562_silent() -> None:
    assert coded(compile_agents(agent("sales_agent", builtin_tool("web_search"))).diagnostics, "SST-VAL562") == []
