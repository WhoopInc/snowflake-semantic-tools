"""SST-VAL536: the instructions route a topic to a tool whose description excludes it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents
from tests.helpers.diagnostic_filters import coded

CHART = builtin_tool(description="Draws charts of returned rows. Do not use for revenue questions.")


def test_sst_val536_fires() -> None:
    model = agent("sales_agent", CHART, orchestration_instructions="Send revenue questions to data_to_chart.")
    [diagnostic] = coded(compile_agents(model).diagnostics, "SST-VAL536")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': instructions route revenue questions to 'data_to_chart', documented as excluding it"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val536_silent() -> None:
    model = agent("sales_agent", CHART, orchestration_instructions="Send trend questions to data_to_chart.")
    assert coded(compile_agents(model).diagnostics, "SST-VAL536") == []
