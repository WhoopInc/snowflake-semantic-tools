"""SST-VAL607: a routine member's signature disagrees with the input schema of the tool calling it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import (
    agent,
    catalog,
    compile_agents,
    found,
    generic_tool,
    parameter,
    procedure_member,
)


def _signed(sql_type: str) -> list[Diagnostic]:
    member = procedure_member(signature=(parameter("ORDER_ID", sql_type),))
    return found(compile_agents(agent("sales_agent", generic_tool()), tools=catalog(member)).diagnostics, "SST-VAL607")


def test_sst_val607_fires() -> None:
    [diagnostic] = _signed("NUMBER")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "tool member 'lookup': signature (ORDER_ID NUMBER) differs from input_schema (order_id string)"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val607_silent() -> None:
    assert _signed("VARCHAR(64)") == []
