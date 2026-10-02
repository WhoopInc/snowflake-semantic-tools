"""SST-VAL528: an agent toolset makes the tool surface non-static."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import ORIGIN, agent, builtin_tool, compile_agents, found

HELPER = {"helper": QualifiedName.parse("DB.S.HELPER")}


def test_sst_val528_fires() -> None:
    delegate = AgentTool("agent", ORIGIN, name="helper", description="Delegate support.", agent_ref="helper")
    [diagnostic] = found(compile_agents(agent("sales_agent", delegate), agents=HELPER).diagnostics, "SST-VAL528")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent' declares an agent toolset"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val528_silent() -> None:
    assert found(compile_agents(agent("sales_agent", builtin_tool()), agents=HELPER).diagnostics, "SST-VAL528") == []
