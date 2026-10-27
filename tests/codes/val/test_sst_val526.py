"""SST-VAL526: a generic tool declares no object input schema."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, procedure_member
from tests.helpers.diagnostic_filters import coded


def test_sst_val526_fires() -> None:
    model = agent("sales_agent", generic_tool(input_schema=MappingProxyType({"type": "string"})))
    [diagnostic] = coded(compile_agents(model, tools=catalog(procedure_member())).diagnostics, "SST-VAL526")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'lookup' input_schema is string"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val526_silent() -> None:
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    assert coded(result.diagnostics, "SST-VAL526") == []
