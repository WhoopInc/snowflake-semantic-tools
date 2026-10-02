"""SST-VAL535: the instructions name a tool the agent does not have."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents, found


def test_sst_val535_fires() -> None:
    model = agent("sales_agent", builtin_tool(), orchestration_instructions="Use the order_lookup tool for one order.")
    [diagnostic] = found(compile_agents(model).diagnostics, "SST-VAL535")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': instructions name tool 'order_lookup', absent from tools:"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val535_silent() -> None:
    model = agent("sales_agent", builtin_tool(), orchestration_instructions="Use the `data_to_chart` tool for trends.")
    assert found(compile_agents(model).diagnostics, "SST-VAL535") == []
