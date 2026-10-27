"""SST-VAL549: two agents share a display name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val549_fires() -> None:
    first = agent("sales_agent", profile=AgentProfile(display_name="Analyst"))
    second = agent("ops_agent", profile=AgentProfile(display_name="analyst"))
    [diagnostic] = coded(compile_agents(first, second).diagnostics, "SST-VAL549")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'ops_agent': display_name 'analyst' is shared with sales_agent"


def test_sst_val549_silent() -> None:
    first = agent("sales_agent", profile=AgentProfile(display_name="Sales"))
    second = agent("ops_agent", profile=AgentProfile(display_name="Operations"))
    assert coded(compile_agents(first, second).diagnostics, "SST-VAL549") == []
