"""SST-VAL528: an agent toolset makes the tool surface non-static."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.model.agent import AgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.agent_builders import ORIGIN, agent, builtin_tool, compile_agents
from tests.helpers.diagnostic_filters import coded

HELPERS = {"helper": QualifiedName.parse("DB.S.HELPER"), "other": QualifiedName.parse("DB.S.OTHER")}


def test_sst_val528_fires() -> None:
    toolset = AgentTool("agent_toolset", ORIGIN, name="helper", description="Support tools.", agent_ref="helper")
    [diagnostic] = coded(compile_agents(agent("sales_agent", toolset), agents=HELPERS).diagnostics, "SST-VAL528")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent' inherits the tools of another agent through toolset 'helper'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val528_fires_once_per_toolset_with_its_own_fingerprint() -> None:
    # Two toolsets on one agent must not share a fingerprint, or a baseline could hold only one.
    first = AgentTool("agent_toolset", ORIGIN, agent_ref="helper")
    second = AgentTool("agent_toolset", ORIGIN, agent_ref="other")
    found = coded(compile_agents(agent("sales_agent", first, second), agents=HELPERS).diagnostics, "SST-VAL528")
    assert [item.context["name"] for item in found] == ["helper", "other"]
    assert len({stable_fingerprint(item) for item in found}) == 2


def test_an_agent_toolset_needs_no_description() -> None:
    # Snowflake replaces a toolset with the tools it inherits, so the orchestrator never reads one.
    toolset = AgentTool("agent_toolset", ORIGIN, agent_ref="helper")
    assert coded(compile_agents(agent("sales_agent", toolset), agents=HELPERS).diagnostics, "SST-VAL518") == []


def test_sst_val528_silent() -> None:
    assert coded(compile_agents(agent("sales_agent", builtin_tool()), agents=HELPERS).diagnostics, "SST-VAL528") == []
