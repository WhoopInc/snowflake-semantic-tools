"""SST-VAL545: tool_not_accessible holds a value Snowflake does not take."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, compile_agents, found


def test_sst_val545_fires() -> None:
    [diagnostic] = found(compile_agents(agent("sales_agent", tool_not_accessible="skip")).diagnostics, "SST-VAL545")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool_not_accessible is 'skip'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val545_silent() -> None:
    assert found(compile_agents(agent("sales_agent", tool_not_accessible="legacy")).diagnostics, "SST-VAL545") == []
