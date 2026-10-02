"""SST-VAL610: an agent reaches a defined procedure that runs with its owner's rights."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import (
    agent,
    catalog,
    compile_agents,
    compile_tools,
    cross,
    found,
    generic_tool,
    procedure_member,
)


def _reached(execute_as: str) -> list[Diagnostic]:
    tools = catalog(procedure_member(reference=False, execute_as=execute_as))
    results = (compile_tools(tools.members), compile_agents(agent("sales_agent", generic_tool()), tools=tools))
    return found(cross(*results), "SST-VAL610")


def test_sst_val610_fires() -> None:
    [diagnostic] = _reached("OWNER")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "tool member 'lookup' runs as owner and is reachable from agent 'sales_agent'"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val610_silent() -> None:
    assert _reached("caller") == []
