"""SST-VAL623: a generic agent tool calls a routine that returns a table."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, parameter, procedure_member
from tests.helpers.diagnostic_filters import coded


def _found(returns: str, name: str = "lookup") -> list[Diagnostic]:
    member = procedure_member(name, reference=False, returns=returns, signature=(parameter("order_id", "VARCHAR"),))
    tool = generic_tool(backing=name)
    return coded(compile_agents(agent("sales_agent", tool), tools=catalog(member)).diagnostics, "SST-VAL623")


def test_sst_val623_fires() -> None:
    [diagnostic] = _found("TABLE (order_id VARCHAR, tier VARCHAR)")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': generic tool 'lookup' calls 'lookup', which returns a table"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val623_silent() -> None:
    assert _found("VARCHAR") == []
    # A routine named after a reserved word still returns one value.
    assert _found("VARCHAR", name="table") == []
