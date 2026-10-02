"""SST-VAL524: a column descriptor is malformed."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, search_member, search_tool


def _described(kind: str) -> list[Diagnostic]:
    columns = {"BODY": {"description": "Text.", "type": kind, "searchable": True, "filterable": False}}
    model = agent("sales_agent", search_tool(columns_and_descriptions=MappingProxyType(columns)))
    return found(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL524")


def test_sst_val524_fires() -> None:
    [diagnostic] = _described("text")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "agent 'sales_agent': tool 'docs' column 'BODY': type is 'text', not string or datetime"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val524_silent() -> None:
    assert _described("string") == []
