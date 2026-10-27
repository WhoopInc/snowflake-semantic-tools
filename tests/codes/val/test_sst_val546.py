"""SST-VAL546: analytical search is enabled with no Cortex Search tool."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, search_member, search_tool
from tests.helpers.diagnostic_filters import coded


def test_sst_val546_fires() -> None:
    [diagnostic] = coded(compile_agents(agent("sales_agent", analytical_search=True)).diagnostics, "SST-VAL546")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': analytical_search is true and no cortex_search tool is declared"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val546_silent() -> None:
    model = agent("sales_agent", search_tool(), analytical_search=True)
    assert coded(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL546") == []
