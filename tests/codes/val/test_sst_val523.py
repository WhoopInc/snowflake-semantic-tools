"""SST-VAL523: a search filter names a column not marked filterable."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, search_member, search_tool


def _filtered(column: str) -> list[object]:
    model = agent("sales_agent", search_tool(filter=MappingProxyType({"@and": [{"@eq": {column: "x"}}]})))
    return list(found(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL523"))


def test_sst_val523_fires() -> None:
    [diagnostic] = _filtered("DOC_NAME")
    assert isinstance(diagnostic, Diagnostic)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'docs' filters on 'DOC_NAME', not marked filterable"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val523_silent() -> None:
    assert _filtered("DOC_ID") == []
