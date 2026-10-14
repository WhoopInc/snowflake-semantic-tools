"""SST-REF020: a search, generic, or toolset tool names a tool member of a type it cannot use."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from tests.helpers.agent_context import agent_context, search_member, tools
from tests.helpers.diagnostic_filters import coded

ORIGIN = Origin("agents/router/agent.yml", 4, 5)
ROUTER = AgentModel("router", ORIGIN, ("agents/router/agent.yml",))


def test_sst_ref020_fires() -> None:
    _, diagnostics = resolve_tool(
        ROUTER,
        AgentTool(
            "cortex_search", ORIGIN, name="search", description="Docs.", backing=("platform", "search"), max_results=3
        ),
        agent_context(catalog=tools(search_member("procedure"))),
    )
    [diagnostic] = coded(diagnostics, "SST-REF020")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{{ tool('search') }} resolves to procedure, expected cortex_search_service"
    assert diagnostic.subject == "agent:router"


def test_sst_ref020_silent() -> None:
    resolved, diagnostics = resolve_tool(
        ROUTER,
        AgentTool(
            "cortex_search", ORIGIN, name="search", description="Docs.", backing=("platform", "search"), max_results=3
        ),
        agent_context(catalog=tools(search_member())),
    )
    assert resolved is not None
    assert coded(diagnostics, "SST-REF020") == []


def test_sst_ref020_fires_for_an_agent_toolset_over_a_member_that_is_not_an_agent() -> None:
    resolved, diagnostics = resolve_tool(
        ROUTER, AgentTool("agent_toolset", ORIGIN, name="search"), agent_context(catalog=tools(search_member()))
    )
    assert resolved is None
    [diagnostic] = coded(diagnostics, "SST-REF020")
    assert diagnostic.message == "{{ tool('search') }} resolves to cortex_search_service, expected agent"


def test_sst_ref020_silent_for_an_agent_toolset_over_an_agent_member() -> None:
    resolved, diagnostics = resolve_tool(
        ROUTER, AgentTool("agent_toolset", ORIGIN, name="search"), agent_context(catalog=tools(search_member("agent")))
    )
    assert resolved is not None
    assert dict(resolved.resources) == {"agent_name": "DB.S.SEARCH"}
    assert resolved.description == "Search the docs."
    assert coded(diagnostics, "SST-REF020") == []
