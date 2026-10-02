"""SST-VAL615: an agent tool overrides a value its member declares."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, generic_tool, procedure_member


def _warehoused(warehouse: str) -> list[Diagnostic]:
    tool = generic_tool(warehouse=warehouse)
    return found(
        compile_agents(agent("sales_agent", tool), tools=catalog(procedure_member())).diagnostics, "SST-VAL615"
    )


def test_sst_val615_fires() -> None:
    [diagnostic] = _warehoused("BIG_WH")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "agent 'sales_agent': tool 'lookup' overrides warehouse"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val615_silent() -> None:
    # Repeating the member's own value overrides nothing.
    assert _warehoused("WH") == []
