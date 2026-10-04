"""SST-VAL520: an Analyst tool is given a name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val520_fires() -> None:
    [diagnostic] = coded(compile_agents(agent("sales_agent", analyst_tool(name="sales"))).diagnostics, "SST-VAL520")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'sales' declares 1 semantic views"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val520_silent() -> None:
    assert coded(compile_agents(agent("sales_agent", analyst_tool())).diagnostics, "SST-VAL520") == []
