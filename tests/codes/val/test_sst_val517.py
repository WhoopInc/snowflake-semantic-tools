"""SST-VAL517: a web_search tool is not named web_search."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents, found


def test_sst_val517_fires() -> None:
    [diagnostic] = found(
        compile_agents(agent("sales_agent", builtin_tool("web_search", name="search"))).diagnostics, "SST-VAL517"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': web_search tool is named 'search'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val517_silent() -> None:
    assert found(compile_agents(agent("sales_agent", builtin_tool("web_search"))).diagnostics, "SST-VAL517") == []
