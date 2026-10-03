"""SST-VAL532: a referenced routine's live signature disagrees with the input schema."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, generic_tool, observe, procedure_member
from tests.helpers.snowflake_fake import FakeSnowflake


def _signed(arguments: str) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.existing = {"DB.DEV.LOOKUP"}
    port.show_rows["PROCEDURE DB.DEV.LOOKUP"] = {"arguments": arguments}
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    return found(observe(port, result), "SST-VAL532")


def test_sst_val532_fires() -> None:
    [diagnostic] = _signed("LOOKUP(NUMBER, NUMBER) RETURN VARCHAR")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': 'DB.DEV.LOOKUP' signature (NUMBER, NUMBER) differs from input_schema (order_id string)"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val532_silent() -> None:
    assert _signed("LOOKUP(VARCHAR) RETURN VARCHAR") == []
