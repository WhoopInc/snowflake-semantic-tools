"""SST-VAL530: a tool_resources key names no rendered tool."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, search_member, search_tool
from tests.helpers.diagnostic_filters import coded


def _renamed(spec: dict[str, object]) -> list[Diagnostic]:
    model = agent("sales_agent", search_tool(tool_spec_passthrough=MappingProxyType(spec)))
    return coded(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL530")


def test_sst_val530_fires() -> None:
    [diagnostic] = _renamed({"name": "documents"})
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool_resources key 'docs' matches no tools[].name"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val530_silent() -> None:
    assert _renamed({"title": "Documents"}) == []
