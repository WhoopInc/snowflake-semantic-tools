"""SST-VAL520: an Analyst tool declares no semantic view."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val520_fires() -> None:
    [diagnostic] = coded(
        compile_agents(agent("sales_agent", analyst_tool(name="sales", semantic_view=None))).diagnostics, "SST-VAL520"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'sales' declares no semantic view"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val520_silent() -> None:
    assert coded(compile_agents(agent("sales_agent", analyst_tool())).diagnostics, "SST-VAL520") == []
    # A name is the tool's own, kept as authored; without one the tool is named after its view.
    named = compile_agents(agent("sales_agent", analyst_tool(name="query_sales")))
    assert coded(named.diagnostics, "SST-VAL520") == []
    [compiled] = [item for item in named.compiled if isinstance(item, CompiledAgent)]
    assert '"name": "query_sales"' in compiled.payload
    assert '"query_sales": {' in compiled.payload
