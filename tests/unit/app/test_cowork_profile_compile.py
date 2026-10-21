"""Compiling CoWork profiles (a Snowflake private preview): subagents, settings, checks, and publication."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.app.compile.cowork_profiles import (
    CompileCoworkProfiles,
    CoworkCompileContext,
    for_cowork_publication,
)
from snowflake_semantic_tools.app.compile.cowork_profiles.subagents import resolve_subagents
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.compile.staged_agent import AgentObjectMetadata, agent_programs
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import AgentProfile, AgentSkill
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.sql import stage_path
from tests.helpers.agent_context import agent_context
from tests.helpers.compile_builders import CHANNELS, skill
from tests.helpers.cowork_profiles import (
    LISTED,
    agent_member,
    compile_profiles,
    compiled_profiles,
    cowork_profile,
    described_agent,
    router_entries,
    subagent,
)
from tests.helpers.project_inputs import InMemoryProjectInputs

STAGE = QualifiedName.parse("DB.S.SPECS")


def codes(*profiles: CoworkProfileModel, **fields: Any) -> list[str]:
    return [item.code for item in compile_profiles(*profiles, **fields).diagnostics]


def test_a_subagent_takes_its_agents_display_name_comment_and_sample_questions() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(),))
    [profile] = compiled_profiles(result)
    assert router_entries(profile) == [
        {
            "name": "DB.SCH.SALES",
            "display_name": "Sales",
            "description": "Pipeline and quota.",
            "source": {
                "type": "DATA_AGENT",
                "fqn": "DB.SCH.SALES",
                "sample_questions": ["What closed last week?"],
            },
        }
    ]
    assert profile.resolved.depends_on == ("agent:sales",)
    assert profile.target.sql == "SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"
    assert [item.code for item in result.diagnostics] == ["SST-VAL870"]


def test_an_agent_with_no_display_name_is_shown_by_its_name() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(profile=AgentProfile()),))
    [entry] = router_entries(compiled_profiles(result)[0])
    assert entry["display_name"] == "sales"


def test_what_the_entry_writes_overrides_the_agent_and_an_empty_list_offers_no_questions() -> None:
    entry = subagent(display_name="Deals", description="Closed deals only.", sample_questions=())
    result = compile_profiles(cowork_profile("everyone", entry), agents=(described_agent(),))
    [rendered] = router_entries(compiled_profiles(result)[0])
    assert (rendered["display_name"], rendered["description"]) == ("Deals", "Closed deals only.")
    assert rendered["source"] == {"type": "DATA_AGENT", "fqn": "DB.SCH.SALES", "sample_questions": []}


def test_a_tool_subagent_routes_to_its_members_relation_and_adds_no_dependency() -> None:
    entry = subagent("partner", "tool", display_name="Partner", description="Partner deals.")
    result = compile_profiles(cowork_profile("everyone", entry), members=(agent_member(),))
    [profile] = compiled_profiles(result)
    assert router_entries(profile) == [
        {
            "name": "OTHER.SCH.PARTNER",
            "display_name": "Partner",
            "description": "Partner deals.",
            "source": {"type": "DATA_AGENT", "fqn": "OTHER.SCH.PARTNER", "sample_questions": []},
        }
    ]
    assert profile.resolved.depends_on == ()
    assert profile.rendered_artifact.references == ("tool_ref:partner",)
    # The profile names the member, so it is referenced.
    assert "SST-CFG018" not in [item.code for item in result.diagnostics]


def test_each_target_routes_to_where_the_agent_publishes_for_that_target() -> None:
    tree = {**LISTED, "agents": {"+schema": "AGENTS"}}
    prod = TargetIdentity("dev", "ACCOUNT", Identifier.parse("PRODDB"), Identifier.parse("SCH"), None, "WH")
    for identity, expected in ((None, "DB.AGENTS.SALES"), (prod, "PRODDB.AGENTS.SALES")):
        result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree=tree, identity=identity)
        [entry] = router_entries(compiled_profiles(result)[0])
        assert (entry["name"], entry["source"]["fqn"]) == (expected, expected)  # type: ignore[index]


def test_the_profile_pins_its_skills_and_depends_on_its_agents_then_them() -> None:
    sales = QualifiedName.parse("DB.S.SALES")
    profile = cowork_profile(skills=(AgentSkill("", "CORTEX_EXTENSION", "semantics", "", ref="skill"),))
    context = CoworkCompileContext(
        agent_context(agents={"sales": sales}), {"sales": described_agent()}, {}, "DB", "PROFILES"
    )
    result = CompileCoworkProfiles((profile,), DiagnosticBag(), context).run_result()
    [compiled] = compiled_profiles(result)
    assert compiled.target.sql == "DB.PROFILES.EVERYONE"
    assert compiled.resolved.model.skills == (
        AgentSkill("semantics", "CORTEX_EXTENSION", "DB.S.SEMANTICS", "SST_ABCDEF012345", ref="skill"),
    )
    assert compiled.resolved.depends_on == ("agent:sales", "skill:semantics")
    assert compiled.rendered_artifact.depends_on == ("agent:sales", "skill:semantics")


def test_settings_give_the_profile_its_model_alias_and_tags_for_the_target() -> None:
    tree = {
        **LISTED,
        "snowflake": {"orchestration_models": ["auto", "claude-4-sonnet"]},
        "tags": {"tier": {"fqn": "DB.S.TIER"}},
        "cowork_profiles": {
            "+database": "SNOWFLAKE_INTELLIGENCE",
            "+schema": "PROFILES",
            "+orchestration_model": "claude-4-sonnet",
            "+alias": "{{ target.schema }}",
            "+tags": [{"name": "{{ tag('tier') }}", "value": "gold"}],
        },
    }
    result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree=tree)
    [profile] = compiled_profiles(result)
    model = profile.resolved.model
    assert (model.orchestration_model, model.alias, model.tags) == ("claude-4-sonnet", "SCH", (("DB.S.TIER", "gold"),))
    assert '"orchestration": "claude-4-sonnet"' in profile.payload
    assert {item.key: item.value for item in profile.settings}["enabled"] is True
    assert profile.rendered_artifact.desired_alias == "SCH"


def test_an_unlisted_model_and_an_unusable_tag_are_reported_as_an_agents_are() -> None:
    profile = cowork_profile(
        settings=MappingProxyType(
            {
                "orchestration_model": Setting("orchestration_model", "gpt", "config"),
                "tags": Setting("tags", [{"name": "{{ tag('missing') }}", "value": "x"}], "config"),
            }
        )
    )
    assert codes(profile, agents=(described_agent(),)) == ["SST-REF028", "SST-VAL543"]


def test_the_rendered_artifact_is_an_agent_object_of_its_own_kind() -> None:
    [profile] = compiled_profiles(compile_profiles(cowork_profile(), agents=(described_agent(),)))
    rendered = profile.rendered_artifact
    assert (profile.name, profile.artifact_key, profile.artifact_type) == (
        "everyone",
        "cowork_profile:everyone",
        "cowork_profile",
    )
    assert (rendered.artifact_type, rendered.object_type, rendered.render_dialect) == (
        "cowork_profile",
        "AGENT",
        "json",
    )
    assert rendered.fingerprint == profile.definition_fingerprint != profile.spec_fingerprint
    assert profile.source_files == ("cowork_profiles/everyone/cowork_profile.yml",)
    assert profile.member_keys == profile.referenced_models == ()


def test_publication_stages_the_spec_under_the_cowork_profiles_folder_with_the_agent_programs() -> None:
    [compiled] = compiled_profiles(compile_profiles(cowork_profile(), agents=(described_agent(),)))
    staged = for_cowork_publication(compiled, stage=STAGE, git_sha="abc1234")
    rendered = staged.rendered_artifact
    assert staged.stage_path == "DB.S.SPECS/cowork-profiles/everyone/abc1234"
    assert rendered.upload_path == "@DB.S.SPECS/cowork-profiles/everyone/abc1234/agent_spec.yaml"
    assert rendered.upload_content == compiled.payload.encode("utf-8")
    programs = agent_programs(
        AgentObjectMetadata(alias=None, tags=(), profile=AgentProfile(), secure=None),
        compiled.target,
        (stage_path(STAGE, "cowork-profiles/everyone/abc1234/"), "abc1234"),
        temporary=False,
        payload=compiled.payload,
        comment=None,
    )
    assert (rendered.create_statements, rendered.update_statements, rendered.update_live_statements) == (
        programs.create,
        programs.update,
        programs.update_live,
    )
    dirty = for_cowork_publication(compiled, stage=STAGE, git_sha="abc1234", dirty=True)
    assert "git:abc1234-dirty" in str(dirty.rendered_artifact.create_statements[1])


def test_publishing_writes_the_marked_comment_first_and_expects_it_back() -> None:
    [compiled] = compiled_profiles(compile_profiles(cowork_profile(comment="Everyone."), agents=(described_agent(),)))
    published = for_cowork_publication(compiled, stage=STAGE, git_sha="abc1234").rendered_for_publish("0" * 64)
    assert published.expected_marker is not None
    assert published.fingerprint == compiled.definition_fingerprint
    assert "COMMENT = 'Everyone." in str(published.create_statements[0])
    assert str(published.update_statements[0]).startswith(f"ALTER AGENT {compiled.target.sql} SET COMMENT")


def test_an_agent_the_project_does_not_publish_enabled_keeps_the_profile_back() -> None:
    disabled = described_agent(enabled=False)
    for agents in ((), (disabled,)):
        result = compile_profiles(cowork_profile(), agents=agents)
        [found] = [item for item in result.diagnostics if item.code == "SST-REF012"]
        assert (found.subject, found.origin) == ("cowork_profile:everyone", subagent().origin)
        assert compiled_profiles(result) == ()


def test_a_deprecated_agent_is_reported_and_still_routed_to() -> None:
    context = CoworkCompileContext(
        agent_context(agents={"sales": QualifiedName.parse("DB.S.SALES")}),
        {"sales": described_agent(deprecated=True)},
        {},
        "SNOWFLAKE_INTELLIGENCE",
        "PROFILES",
    )
    resolved, problems = resolve_subagents(cowork_profile(), context)
    assert [item.code for item in problems] == ["SST-VAL875"]
    assert [item.fqn.sql for item in resolved] == ["DB.S.SALES"]
    assert compiled_profiles(CompileCoworkProfiles((cowork_profile(),), DiagnosticBag(), context).run_result()) == ()


def test_a_tool_subagent_must_name_an_agent_member_that_exists() -> None:
    unknown = compile_profiles(cowork_profile("everyone", subagent("nobody", "tool")))
    [missing] = [item for item in unknown.diagnostics if item.code == "SST-REF010"]
    assert (missing.subject, missing.origin) == ("cowork_profile:everyone", subagent().origin)
    assert compiled_profiles(unknown) == ()
    search = agent_member("docs", kind="cortex_search_service")
    assert "SST-VAL872" in codes(cowork_profile("everyone", subagent("docs", "tool")), members=(search,))


def test_a_member_with_no_relation_for_the_target_is_reported_against_the_member() -> None:
    elsewhere = replace(agent_member(), relations=MappingProxyType({"prod": "P.S.PARTNER"}))
    described = subagent("partner", "tool", display_name="Partner", description="Partner deals.")
    result = compile_profiles(cowork_profile("everyone", described), members=(elsewhere,))
    [found] = [item for item in result.diagnostics if item.code == "SST-REF018"]
    assert found.subject == "tool_ref:partner"
    [profile] = compiled_profiles(result)
    assert router_entries(profile) == []


def test_a_subagent_without_a_display_name_or_description_after_its_defaults_is_reported() -> None:
    external = cowork_profile("everyone", subagent("partner", "tool"))
    assert [
        item.context["field"]
        for item in compile_profiles(external, members=(agent_member(),)).diagnostics
        if item.code == "SST-VAL874"
    ] == ["display_name", "description"]
    blank = described_agent(comment="  ")
    assert codes(cowork_profile(), agents=(blank,)).count("SST-VAL874") == 1


def test_two_calls_reaching_one_agent_are_reported_and_a_loaded_repeat_is_not_reported_again() -> None:
    member = agent_member("sales_member", relation="db.sch.sales")
    described = subagent("sales_member", "tool", line=6, display_name="S", description="S.")
    reached = compile_profiles(
        cowork_profile("everyone", subagent(), described), agents=(described_agent(),), members=(member,)
    )
    [found] = [item for item in reached.diagnostics if item.code == "SST-VAL873"]
    assert found.message == "cowork profile 'everyone': spec.subagents[1] names tool('sales_member') again"
    repeat = subagent("SALES", line=6)
    loaded = D(
        "SST-VAL873", subject="cowork_profile:everyone", artifact="everyone", index=1, function="agent", name="SALES"
    )
    inputs = InMemoryProjectInputs(
        tree=dict(LISTED),
        agent_models=(described_agent(),),
        cowork_profile_models=(cowork_profile("everyone", subagent(), repeat),),
        cowork_profile_diagnostics=DiagnosticBag((loaded,)),
    )
    assert [item.code for item in CompileProject(inputs).run().diagnostics].count("SST-VAL873") == 1


def test_every_compiled_profile_is_noted_as_preview_and_one_outside_the_profiles_schema_as_unlisted() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree={"cowork_profiles": {"+schema": "P"}})
    assert [(item.code, item.subject) for item in result.diagnostics] == [
        ("SST-VAL870", "cowork_profile:everyone"),
        ("SST-VAL878", "cowork_profile:everyone"),
    ]


def test_a_profile_and_an_agent_in_one_schema_and_an_agent_where_cowork_lists_profiles() -> None:
    beside = compile_profiles(cowork_profile(), agents=(described_agent(),), tree={})
    assert [item.code for item in beside.diagnostics] == ["SST-VAL870", "SST-VAL878", "SST-VAL879"]
    tree = {**LISTED, "agents": {"+database": "SNOWFLAKE_INTELLIGENCE", "+schema": "PROFILES"}}
    inside = compile_profiles(agents=(described_agent(),), tree=tree)
    assert [item.code for item in inside.diagnostics] == ["SST-VAL880"]


def test_profiles_repeating_a_name_or_display_name_are_reported_and_a_disabled_one_is_left_out() -> None:
    shown = AgentProfile(display_name="Everyone")
    profiles = (
        cowork_profile(display=shown),
        cowork_profile("EVERYONE", display=shown),
        cowork_profile("off", enabled=False),
    )
    result = compile_profiles(*profiles, agents=(described_agent(),))
    assert [item.code for item in result.diagnostics if item.code in ("SST-VAL001", "SST-VAL549")] == [
        "SST-VAL001",
        "SST-VAL549",
    ]
    assert "cowork_profile:off" not in [item.artifact_key for item in result.compiled]


def test_passthrough_keys_are_unmodelled_as_an_agents_are() -> None:
    profile = cowork_profile(passthrough=MappingProxyType({"mcp_servers": []}))
    assert "SST-VAL014" in codes(profile, agents=(described_agent(),))


def test_a_skill_only_a_profile_pins_has_a_consumer() -> None:
    pin = AgentSkill("", "CORTEX_EXTENSION", "month-close", "", ref="skill")
    inputs = InMemoryProjectInputs(
        tree={**LISTED, **CHANNELS},
        skills=SkillCatalog((skill("month-close"),)),
        agent_models=(described_agent(),),
        cowork_profile_models=(cowork_profile(skills=(pin,)),),
    )
    result = CompileProject(inputs).run()
    assert "SST-VAL804" not in [item.code for item in result.diagnostics]
    [profile] = compiled_profiles(result)
    assert profile.resolved.skill_dependencies == ("skill:month-close",)
