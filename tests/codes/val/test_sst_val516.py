"""SST-VAL516: a tool declares a key its type does not take."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents, found


def test_sst_val516_fires() -> None:
    model = agent("sales_agent", analyst_tool(max_results=5))
    [diagnostic] = found(compile_agents(model).diagnostics, "SST-VAL516")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'sales_agent': tool 'SALES' of type cortex_analyst_text_to_sql declares 'max_results'"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val516_silent() -> None:
    assert found(compile_agents(agent("sales_agent", analyst_tool(query_timeout=30))).diagnostics, "SST-VAL516") == []
