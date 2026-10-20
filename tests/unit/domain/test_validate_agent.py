"""The agent checks report name clashes, agent-wide rules, spec size, delegation cycles, and unused extensions."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentProfile, ResolvedAgent, ResolvedAgentTool
from snowflake_semantic_tools.domain.validate.agent import (
    SPEC_LIMIT_BYTES,
    SPEC_WARNING_BYTES,
    AgentIdentities,
    agent_rules,
    delegation_cycle,
    delegations,
    published_agent_rules,
    spec_size,
    tool_name_clashes,
    unreferenced_extensions,
)
from tests.helpers.diagnostic_filters import codes

MODELS = frozenset(("auto",))


def agent(name: str = "a", **fields: Any) -> AgentModel:
    return AgentModel(name, Origin("agent.yml"), ("agent.yml",), **fields)


def tool(name: str, type_: str = "data_to_chart", **resources: object) -> ResolvedAgentTool:
    return ResolvedAgentTool(type_, name, "d", resources=resources)


def test_identities_report_a_repeated_name_and_display_name_ignoring_case() -> None:
    identities = AgentIdentities()
    assert identities.claim(agent("Sales", profile=AgentProfile(display_name="Sales bot"))) == ()
    assert identities.claim(agent("plain")) == ()
    repeated = identities.claim(agent("SALES", profile=AgentProfile(display_name="sales BOT")))
    assert codes(repeated) == ["SST-VAL001", "SST-VAL549"]
    assert set(identities.names) == {"sales", "plain"}


def test_tool_names_clash_exactly_or_only_by_case() -> None:
    model = agent()
    assert tool_name_clashes(model, (tool("a"), tool("b"))) == []
    assert codes(tool_name_clashes(model, (tool("a"), tool("a"), tool("A")))) == [
        "SST-VAL514",
        "SST-VAL515",
    ]


def test_agent_rules_pass_a_plain_agent() -> None:
    assert agent_rules(agent(tool_not_accessible="reject"), MODELS, ()) == []


def test_agent_rules_report_each_agent_wide_problem_in_order() -> None:
    model = agent(
        alias="live",
        tags=(("db.sch.tag", "v"), ("x.y", "v"), ("", "v")),
        orchestration_model="claude",
        tool_not_accessible="maybe",
        analytical_search=True,
    )
    assert codes(agent_rules(model, MODELS, (tool("t"),))) == [
        "SST-PRS025",
        "SST-PRS005",
        "SST-VAL543",
        "SST-VAL545",
        "SST-VAL546",
    ]


def test_an_alias_is_a_plain_identifier_and_a_one_part_tag_name_may_be_plain() -> None:
    # Quoting an alias would make it case-sensitive, so a quoted one is refused; a tag is not.
    model = agent(alias='"Prod"', tags=(("owner", "v"), ("bad name", "v")))
    assert codes(agent_rules(model, MODELS, ())) == ["SST-PRS005", "SST-PRS005"]


def test_analytical_search_is_satisfied_by_a_cortex_search_tool() -> None:
    model = agent(alias="bad name", analytical_search=True)
    assert codes(agent_rules(model, MODELS, (tool("s", "cortex_search"),))) == ["SST-PRS005"]


def test_spec_size_errors_at_the_limit_and_warns_near_it() -> None:
    resolved = ResolvedAgent(agent(), ())
    assert codes(spec_size(resolved, "x" * SPEC_LIMIT_BYTES)) == ["SST-VAL511"]
    assert codes(spec_size(resolved, "x" * SPEC_WARNING_BYTES)) == ["SST-VAL512"]
    assert spec_size(resolved, "x") == ()


def test_delegations_list_each_agent_toolset_target_casefolded() -> None:
    toolsets = (tool("d", "agent_toolset", agent_name="DB.SCH.Other"), tool("e", "agent_toolset"))
    # Snowflake's client-side `agent` tool names no agent whose tools this agent inherits.
    resolved = ResolvedAgent(agent(), (tool("t"), *toolsets, tool("f", "agent", identifier="DB.SCH.Client")))
    assert delegations(resolved) == ("db.sch.other", "")


def test_delegation_cycle_is_found_through_qualified_and_bare_targets() -> None:
    agents = {name: agent(name) for name in ("a", "b", "c")}
    graph = {"a": ("db.sch.b",), "b": ("a",), "c": ("elsewhere",)}
    assert delegation_cycle(graph, agents) == ("a", "b", "a")


def test_delegation_without_a_cycle_reports_nothing() -> None:
    agents = {name: agent(name) for name in ("a", "b", "c")}
    assert delegation_cycle({"a": ("b", "c"), "b": ("c",), "c": ()}, agents) == ()


def test_unreferenced_extensions_skip_pinned_and_consumed_ones() -> None:
    pinned = ResolvedAgent(agent(), (), skill_dependencies=("skill:used",))
    published = (("skill:used", "V1"), ("skill:kept", "V1"), ("plugin:idle", "V2"))
    found = unreferenced_extensions((pinned,), published, frozenset(("skill:kept",)))
    assert [(item.code, item.subject) for item in found] == [("SST-VAL804", "plugin:idle")]


def test_identities_name_the_kind_they_track_in_a_repeated_name() -> None:
    identities = AgentIdentities("cowork profile")
    assert identities.claim(agent("Everyone")) == ()
    [repeated] = identities.claim(agent("EVERYONE"))
    assert repeated.message == "cowork profile 'EVERYONE' is declared more than once"


def test_the_rules_every_agent_object_shares_are_the_agent_rules_without_the_agent_own() -> None:
    model = agent(
        alias="live",
        orchestration_model="claude",
        tool_not_accessible="maybe",
        profile=AgentProfile(color="#fff"),
        deprecated=True,
    )
    assert codes(published_agent_rules(model, MODELS)) == ["SST-PRS025", "SST-VAL543", "SST-VAL548", "SST-VAL550"]
    assert codes(agent_rules(model, MODELS, ())) == [
        "SST-PRS025",
        "SST-VAL543",
        "SST-VAL545",
        "SST-VAL548",
        "SST-VAL550",
    ]
    assert published_agent_rules(agent(), MODELS, frozenset(("Icon",))) == []
