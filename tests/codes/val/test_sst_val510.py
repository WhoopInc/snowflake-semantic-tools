"""SST-VAL510: the rendered spec drops a section the agent carries."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val510_fires() -> None:
    nulled = agent("sales_agent", analyst_tool(), passthrough=MappingProxyType({"tools": None}))
    [diagnostic] = coded(compile_agents(nulled).diagnostics, "SST-VAL510")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': rendered spec omits tools"
    assert diagnostic.subject == "agent:sales_agent"
    assert not ERROR_REGISTRY["SST-VAL510"].demotable


def test_sst_val510_silent() -> None:
    kept = agent("sales_agent", analyst_tool(), passthrough=MappingProxyType({"tools": []}))
    assert coded(compile_agents(agent("sales_agent", analyst_tool()), kept).diagnostics, "SST-VAL510") == []
