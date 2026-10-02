"""SST-VAL525: a search tool marks a vector column searchable."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import (
    agent,
    catalog,
    compile_agents,
    compile_tools,
    found,
    observe,
    search_member,
    search_tool,
)
from tests.helpers.app_ports import InMemorySnowflake


class _Columns(InMemorySnowflake):
    def __init__(self, kind: str) -> None:
        super().__init__()
        self.kind = kind

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        del qualified_name
        return (("DOC_ID", "VARCHAR"), ("DOC_NAME", self.kind))


def _searched(kind: str) -> list[Diagnostic]:
    tools = catalog(search_member())
    results = (compile_tools(tools.members), compile_agents(agent("sales_agent", search_tool()), tools=tools))
    return found(observe(_Columns(kind), *results), "SST-VAL525")


def test_sst_val525_fires() -> None:
    [diagnostic] = _searched("VECTOR(FLOAT, 768)")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': tool 'docs' marks vector column 'DOC_NAME' searchable"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val525_silent() -> None:
    assert _searched("VARCHAR") == []
