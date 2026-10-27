"""A CoWork profile's key and the static checks it passes on its own (a Snowflake private preview)."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.cowork_profile import (
    COWORK_PROFILE_SCHEMA,
    COWORK_PROFILE_TOOLS,
    ROUTER_NAME,
    CoworkProfileModel,
    CoworkProfileTool,
    CoworkSubagent,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.validate.cowork_profile import (
    cowork_profile_diagnostics,
    schema_problems,
    unlisted_profile,
)
from tests.helpers.registry_types import artifact, freeze

ORIGIN = Origin("cowork_profiles/everyone/cowork_profile.yml", 1, 1)
AGENT_ORIGIN = Origin("agents/sales/agent.yml", 1, 1)


def _profile(*subagents: CoworkSubagent, tools: tuple[CoworkProfileTool, ...] = ()) -> CoworkProfileModel:
    return CoworkProfileModel("Everyone", ORIGIN, (ORIGIN.file,), subagents=subagents, tools=tools)


def _subagent(name: str, reference: str = "agent", line: int = 3) -> CoworkSubagent:
    return CoworkSubagent(reference, name, Origin(ORIGIN.file, line, 7))


def test_a_profile_is_keyed_by_its_kind_and_casefolded_name() -> None:
    assert _profile().key == "cowork_profile:everyone"
    assert (ROUTER_NAME, COWORK_PROFILE_SCHEMA) == ("router", ("SNOWFLAKE_INTELLIGENCE", "PROFILES"))
    assert frozenset(("web_search", "code_execution")) == COWORK_PROFILE_TOOLS


def test_a_profile_with_no_subagent_names_no_agent_and_one_subagent_passes() -> None:
    [found] = cowork_profile_diagnostics(_profile())
    assert (found.code, found.subject, found.origin) == ("SST-VAL871", "cowork_profile:everyone", ORIGIN)
    assert cowork_profile_diagnostics(_profile(_subagent("sales"))) == ()


def test_a_repeated_subagent_is_reported_at_each_repeat_ignoring_case_but_not_across_functions() -> None:
    subagents = (_subagent("sales"), _subagent("Sales", line=5), _subagent("sales", "tool"), _subagent("SALES", line=9))
    found = cowork_profile_diagnostics(_profile(*subagents))
    assert [(item.code, item.origin) for item in found] == [
        ("SST-VAL873", Origin(ORIGIN.file, 5, 7)),
        ("SST-VAL873", Origin(ORIGIN.file, 9, 7)),
    ]
    assert found[0].message == "cowork profile 'Everyone': spec.subagents[1] names agent('Sales') again"


def test_only_web_search_and_code_execution_ride_beside_the_router() -> None:
    at = Origin(ORIGIN.file, 12, 7)
    tools = (
        CoworkProfileTool("web_search", None, ORIGIN),
        CoworkProfileTool("cortex_analyst_text_to_sql", "analyst", at),
        CoworkProfileTool("code_execution", "run", ORIGIN),
    )
    [found] = cowork_profile_diagnostics(_profile(_subagent("sales"), tools=tools))
    assert (found.code, found.origin) == ("SST-VAL876", at)
    assert found.message == (
        "cowork profile 'Everyone': spec.tools[1] has type 'cortex_analyst_text_to_sql', "
        "not web_search or code_execution"
    )


def test_agents_and_cowork_profiles_share_the_agent_object_type_and_nothing_else_is_shared() -> None:
    assert SEMANTIC_REGISTRY.shared_object_types() == frozenset({"AGENT"})
    assert freeze((artifact("semantic_view"), artifact("agent", 300))).shared_object_types() == frozenset()


def test_a_profile_outside_the_profiles_schema_is_a_rehearsal_compared_as_snowflake_resolves_it() -> None:
    listed = QualifiedName.parse("snowflake_intelligence.profiles.everyone")
    assert unlisted_profile(_profile(), listed) == ()
    [found] = unlisted_profile(_profile(), QualifiedName.parse('DEV."profiles".EVERYONE'))
    assert (found.code, found.subject, found.origin) == ("SST-VAL878", "cowork_profile:everyone", ORIGIN)
    assert found.message == (
        "cowork profile 'Everyone' publishes to DEV.\"profiles\", where CoWork does not list profiles"
    )


def test_an_agent_in_the_profiles_schema_and_a_profile_beside_an_agent_are_reported() -> None:
    sales, support = AgentModel("sales", AGENT_ORIGIN, ()), AgentModel("support", AGENT_ORIGIN, ())
    agents = (
        (sales, QualifiedName.parse("DB.SHARED.SALES")),
        (support, QualifiedName.parse("SNOWFLAKE_INTELLIGENCE.PROFILES.SUPPORT")),
        (AgentModel("later", AGENT_ORIGIN, ()), QualifiedName.parse("db.shared.LATER")),
    )
    profiles = (
        (_profile(), QualifiedName.parse("db.shared.EVERYONE")),
        (CoworkProfileModel("Apart", ORIGIN, ()), QualifiedName.parse("DB.APART.APART")),
    )
    found = schema_problems(agents, profiles)
    assert [(item.code, item.subject) for item in found] == [
        ("SST-VAL880", "agent:support"),
        ("SST-VAL879", "cowork_profile:everyone"),
    ]
    assert found[1].message == "cowork profile 'Everyone' publishes to DB.SHARED, where agent 'sales' publishes too"
    assert schema_problems(agents[:1], profiles[1:]) == ()
