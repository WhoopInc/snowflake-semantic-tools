"""SST-VAL624: a generic agent tool calls a routine whose parameter name must be quoted."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, parameter, procedure_member
from tests.helpers.diagnostic_filters import coded


def _found(parameter_name: str, member_name: str = "lookup") -> list[Diagnostic]:
    member = procedure_member(member_name, reference=False, signature=(parameter(parameter_name, "VARCHAR"),))
    schema = {"type": "object", "properties": {parameter_name: {"type": "string"}}, "required": [parameter_name]}
    tool = generic_tool(backing=member_name, input_schema=MappingProxyType(schema))
    return coded(compile_agents(agent("sales_agent", tool), tools=catalog(member)).diagnostics, "SST-VAL624")


def test_sst_val624_fires() -> None:
    [diagnostic] = _found("select")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'sales_agent': generic tool 'lookup' cannot pass 'lookup' parameter 'select', whose name is quoted"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val624_fires_for_a_name_written_quoted() -> None:
    assert [item.context["field"] for item in _found('"Order Id"')] == ['"Order Id"']


def test_sst_val624_silent() -> None:
    assert _found("selection") == []
    # A routine named after a reserved word is quoted by SST; only its parameters are named by the runtime.
    assert _found("order_id", member_name="table") == []
