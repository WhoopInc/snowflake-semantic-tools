"""SST-VAL625: a generic agent tool calls a routine with an ARRAY, OBJECT or VARIANT parameter."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, parameter, procedure_member
from tests.helpers.diagnostic_filters import coded


def _found(sql_type: str) -> list[Diagnostic]:
    member = procedure_member(reference=False, signature=(parameter("c", sql_type),))
    return coded(compile_agents(agent("sales_agent", generic_tool()), tools=catalog(member)).diagnostics, "SST-VAL625")


@pytest.mark.parametrize("sql_type", ["ARRAY", "OBJECT", "VARIANT", "array(number)"])
def test_sst_val625_fires(sql_type: str) -> None:
    [diagnostic] = _found(sql_type)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"agent 'sales_agent': generic tool 'lookup' cannot pass 'lookup' parameter 'c' of type {sql_type}"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val625_silent() -> None:
    assert _found("VARCHAR(64)") == []
