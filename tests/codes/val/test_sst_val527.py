"""SST-VAL527: a generic tool resolves no warehouse."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, generic_tool, procedure_member


def test_sst_val527_fires() -> None:
    tools = catalog(procedure_member(warehouse=None))
    [diagnostic] = found(
        compile_agents(agent("sales_agent", generic_tool()), tools=tools, warehouse=None).diagnostics, "SST-VAL527"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'lookup' declares no warehouse"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val527_silent() -> None:
    tools = catalog(procedure_member(warehouse=None))
    assert found(compile_agents(agent("sales_agent", generic_tool()), tools=tools).diagnostics, "SST-VAL527") == []
