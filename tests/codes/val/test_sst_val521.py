"""SST-VAL521: a Cortex Search tool omits its search service."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, search_member, search_tool


def test_sst_val521_fires() -> None:
    model = agent("sales_agent", search_tool(backing=()))
    [diagnostic] = found(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL521")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'docs' omits 'search_service'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val521_silent() -> None:
    result = compile_agents(agent("sales_agent", search_tool()), tools=catalog(search_member()))
    assert found(result.diagnostics, "SST-VAL521") == []
