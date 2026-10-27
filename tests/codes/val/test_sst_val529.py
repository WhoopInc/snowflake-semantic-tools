"""SST-VAL529: a built-in tool would emit tool_resources."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val529_fires() -> None:
    model = agent("sales_agent", builtin_tool(passthrough=MappingProxyType({"region": "us"})))
    [diagnostic] = coded(compile_agents(model).diagnostics, "SST-VAL529")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': built-in tool 'data_to_chart' would emit tool_resources"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val529_silent() -> None:
    model = agent("sales_agent", builtin_tool(tool_spec_passthrough=MappingProxyType({"title": "Charts"})))
    assert coded(compile_agents(model).diagnostics, "SST-VAL529") == []
