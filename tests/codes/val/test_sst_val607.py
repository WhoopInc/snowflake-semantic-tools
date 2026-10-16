"""SST-VAL607: a routine member's signature disagrees with the input schema of the tool calling it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, parameter, procedure_member
from tests.helpers.diagnostic_filters import coded


def _signed(sql_type: str) -> list[Diagnostic]:
    member = procedure_member(signature=(parameter("ORDER_ID", sql_type),))
    return coded(compile_agents(agent("sales_agent", generic_tool()), tools=catalog(member)).diagnostics, "SST-VAL607")


def test_sst_val607_fires() -> None:
    [diagnostic] = _signed("NUMBER")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "tool member 'lookup': signature (ORDER_ID NUMBER) differs from input_schema (order_id string)"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val607_silent() -> None:
    assert _signed("VARCHAR(64)") == []


TWO = {
    "type": "object",
    "properties": {"order_total": {"type": "number"}, "tip_pct": {"type": "number"}},
    "required": ["order_total"],
}


def _required(listed: list[str]) -> list[Diagnostic]:
    member = procedure_member(signature=(parameter("order_total", "NUMBER(10, 2)"), parameter("tip_pct", "NUMBER")))
    tool = generic_tool(input_schema={**TWO, "required": listed})
    return coded(compile_agents(agent("sales_agent", tool), tools=catalog(member)).diagnostics, "SST-VAL607")


def test_sst_val607_fires_for_a_parameter_the_schema_does_not_require() -> None:
    [diagnostic] = _required(["order_total"])
    assert diagnostic.message == (
        "tool member 'lookup': signature (order_total NUMBER(10, 2), tip_pct NUMBER) requiring each "
        "differs from input_schema (order_total number, tip_pct number) not requiring tip_pct"
    )


def test_sst_val607_silent_when_the_schema_requires_every_parameter() -> None:
    assert _required(["ORDER_TOTAL", "tip_pct"]) == []
