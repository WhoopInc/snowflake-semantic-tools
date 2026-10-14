"""SST-VAL521: a Cortex Search tool omits its search service."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, search_member, search_tool
from tests.helpers.diagnostic_filters import coded


def test_sst_val521_fires() -> None:
    model = agent("sales_agent", search_tool(backing=None))
    [diagnostic] = coded(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL521")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'docs' omits search_service"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val521_names_every_key_a_tool_omits() -> None:
    model = agent("sales_agent", search_tool(name="", backing=None), generic_tool(name=""))
    messages = sorted(
        diagnostic.message
        for diagnostic in coded(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL521")
    )
    assert messages == [
        "agent 'sales_agent': tool 'cortex_search' omits name, search_service",
        "agent 'sales_agent': tool 'generic' omits name",
    ]


def test_sst_val521_silent() -> None:
    result = compile_agents(agent("sales_agent", search_tool()), tools=catalog(search_member()))
    assert coded(result.diagnostics, "SST-VAL521") == []
