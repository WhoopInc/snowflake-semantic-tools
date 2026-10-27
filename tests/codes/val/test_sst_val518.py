"""SST-VAL518: a tool has no description."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val518_fires() -> None:
    [diagnostic] = coded(compile_agents(agent("sales_agent", builtin_tool(description=None))).diagnostics, "SST-VAL518")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'data_to_chart' has no description"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val518_silent() -> None:
    assert coded(compile_agents(agent("sales_agent", builtin_tool())).diagnostics, "SST-VAL518") == []
