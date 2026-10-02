"""The whole-project compile, driven by an in-memory `ProjectInputs`.

What it reads, in what order, and what it builds.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileArtifacts, CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.project import POSITIONS, CompileProject, consumed_extensions
from snowflake_semantic_tools.app.compile.skills import (
    CatalogChannel,
    CompiledExtension,
    CompileSkills,
    extension_pins,
    unpublished_reasons,
)
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.profile import DesktopProfile, ProfileCatalog, SharedProfile
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.skill import Plugin, SkillCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from tests.helpers.compile_builders import CHANNELS, agent, plugin, skill, view
from tests.helpers.project_inputs import InMemoryProjectInputs, dev_target


def reasons(result: CompileResult) -> dict[str, str]:
    return {
        str(item.context["name"]): str(item.context["reason"])
        for item in result.diagnostics
        if item.code == "SST-VAL856"
    }


def with_broken_skill(*plugins: Plugin) -> SkillCatalog:
    """`draft` and `broken`, the second missing its description as the loader reports it."""
    missing = D(
        "SST-PRS034", origin=Origin("SKILL.md", 1), subject="skill:broken", artifact="skill:broken", field="description"
    )
    return SkillCatalog((skill("draft"), skill("broken", None)), plugins, DiagnosticBag((missing,)))


def test_a_project_without_dbt_reads_only_its_configuration_and_skill_catalog() -> None:
    inputs = InMemoryProjectInputs(
        has_dbt_project=False,
        config_diagnostics=DiagnosticBag((D("SST-CFG003", key="x", origin=Origin("sst_config.yml")),)),
    )

    result = CompileProject(inputs).run()

    assert inputs.reads == ["config", "target", "skill_catalog:skills,plugins"]
    assert result.compiled == ()
    assert [item.code for item in result.diagnostics] == ["SST-CFG003"]


def test_a_dbt_project_is_read_in_a_fixed_order_from_its_configured_roots() -> None:
    inputs = InMemoryProjectInputs(
        tree={
            "project": {"skills_dir": "sk", "agents_dir": "ag", "hooks_dir": "hk", "commands_dir": "cm"},
            "skills": CHANNELS,
        },
        views=SemanticViewProject((view("SALES"),)),
        agent_models=(agent("helper"),),
    )

    CompileProject(inputs).run()

    assert inputs.reads == [
        "config",
        "target",
        "skill_catalog:sk,plugins",
        "profile_catalog:profiles,hk,mcp-servers,cm",
        "load_project",
        "dbt_catalog",
        "tool_catalog",
        "agents:ag",
        "git_sha",
        "eval_catalog",
    ]


def test_results_merge_in_ddl_order_after_the_configuration_and_target_diagnostics() -> None:
    profile = DesktopProfile("analyst", "profiles/analyst", "P.", "Data", ("month-close",), (), (), None, Origin("p"))
    inputs = InMemoryProjectInputs(
        tree={"skills": CHANNELS},
        config_diagnostics=DiagnosticBag((D("SST-CFG003", key="x", origin=Origin("sst_config.yml")),)),
        target_diagnostics=(D("SST-CFG048", target="dev", key="threads"),),
        skills=SkillCatalog((skill("month-close"),)),
        profiles=ProfileCatalog((profile,)),
        views=SemanticViewProject((view("SALES"),)),
        agent_models=(agent("helper", "month-close"),),
    )

    result = CompileProject(inputs).run()

    assert [item.artifact_key for item in result.compiled] == [
        "semantic_view:sales",
        "skill:month-close",
        "profile:analyst",
        "agent:helper",
    ]
    assert [item.code for item in result.diagnostics[:2]] == ["SST-CFG003", "SST-CFG048"]
    skill_target = next(item for item in result.compiled if isinstance(item, CompiledExtension)).release.target
    assert skill_target.sql == "DB.SCH.MONTH_CLOSE"


def test_declared_skills_that_cannot_publish_name_the_reason_to_the_agents_that_reference_them() -> None:
    def compile_with(skills_block: dict[str, object]) -> dict[str, str]:
        inputs = InMemoryProjectInputs(
            tree={"skills": skills_block},
            skills=with_broken_skill(),
            agent_models=(agent("router", "draft", "broken"),),
        )
        return reasons(CompileProject(inputs).run())

    missing = "skills.catalog is not configured"
    assert compile_with({}) == {"draft": missing, "broken": missing}
    assert compile_with({"stage": {"+stage": "P"}}) == {"draft": missing, "broken": "it has errors"}
    assert compile_with({"catalog": {}}) == {
        "draft": "skills.catalog sets no +bundle_stage",
        "broken": "it has errors",
    }
    assert compile_with({"catalog": {"+bundle_stage": "B"}}) == {"broken": "it has errors"}
    bad_prefix = compile_with({"+version_prefix": "1", "catalog": {"+bundle_stage": "B"}})
    assert bad_prefix["draft"] == "the catalog channel cannot publish it"


def test_an_extension_entry_that_cannot_be_qualified_is_unpublished_for_that_reason() -> None:
    inputs = InMemoryProjectInputs(
        tree={"skills": {"extensions": {"glossary": None}}},
        agent_models=(
            AgentModel(
                "router",
                Origin("agent.yml"),
                ("agent.yml",),
                skills=(AgentSkill("glossary", "CORTEX_EXTENSION", "glossary", "V1", ref="extension"),),
            ),
        ),
    )

    result = CompileProject(inputs).run()

    assert reasons(result) == {"glossary": "its skills.extensions entry cannot be qualified"}
    assert result.diagnostics[0].code == "SST-CFG036"


def test_consumed_extensions_resolve_from_fqn_or_default_prefix() -> None:
    config: dict[str, object] = {
        "skills": {
            "extensions": {
                "default_prefix": "{{ target.database }}.SHARED",
                "vendor-pack": None,
                "pinned": {"fqn": "OTHER.EXT.PINNED"},
                "dotted.name": None,
                "broken": {"fqn": "not a name"},
            }
        }
    }
    resolved, diagnostics = consumed_extensions(config, dev_target())
    assert {key: value.sql for key, value in resolved.items()} == {
        "vendor-pack": "DB.SHARED.VENDOR_PACK",
        "pinned": "OTHER.EXT.PINNED",
    }
    assert [(item.code, item.context["name"]) for item in diagnostics] == [
        ("SST-CFG036", "dotted.name"),
        ("SST-CFG036", "broken"),
    ]
    without_prefix, diagnostics = consumed_extensions({"skills": {"extensions": {"x": None}}}, dev_target())
    assert without_prefix == {}
    assert "no default_prefix" in diagnostics[0].message


def test_agents_follow_the_agents_block_and_disabled_agents_are_left_out() -> None:
    tree: dict[str, object] = {
        "agents": {"+database": "{{ target.database }}_AGENTS", "+schema": "BOTS", "+orchestration_model": "fast"},
        "snowflake": {"orchestration_models": ["auto", "fast"]},
    }
    inputs = InMemoryProjectInputs(tree=tree, agent_models=(agent("helper"), agent("retired", enabled=False)))

    result = CompileProject(inputs).run()

    [compiled] = [item for item in result.compiled if isinstance(item, CompiledAgent)]
    assert compiled.rendered_artifact.target.sql == "DB_AGENTS.BOTS.HELPER"
    assert '"orchestration": "fast"' in compiled.payload
    agents, tool_names = inputs.eval_requests[0]
    assert agents == (agent("helper"),) and tool_names == {"helper": ()}
    assert "SST-VAL543" not in {item.code for item in result.diagnostics}

    disabled = InMemoryProjectInputs(tree={"agents": {"+enabled": False}}, agent_models=(agent("helper"),))
    assert [item.artifact_type for item in CompileProject(disabled).run().compiled] == []
    assert disabled.eval_requests[0][0] == ()


def test_only_auto_is_allowed_without_a_list_of_orchestration_models() -> None:
    inputs = InMemoryProjectInputs(agent_models=(agent("helper", model="fast"),))

    result = CompileProject(inputs).run()

    assert "SST-VAL543" in {item.code for item in result.diagnostics}


def test_tools_take_the_tools_block_defaults_and_resolve_dbt_models_to_relations() -> None:
    search = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="docs",
        search_column="body",
    )
    inputs = InMemoryProjectInputs(
        tree={"tools": {"+schema": "TOOLS", "+target_lag": "1 hour", "+warehouse": "{{ target.warehouse }}_XL"}},
        tools=ToolCatalog(
            (ToolGroup("platform", Origin("t"), "tools.yml", members=(search,)),), "dev", frozenset(("dev",))
        ),
        dbt=DbtCatalog("v12", None, None, (DbtModel("model.docs", "docs", "DB.SCH.DOCS", ("ID",), (), ()),)),
    )

    rendered = CompileProject(inputs).run().rendered[0]

    assert rendered.target.sql == "DB.TOOLS.SEARCH"
    assert "WAREHOUSE = WH_XL" in rendered.ddl
    assert "TARGET_LAG = '1 hour'" in rendered.ddl
    assert rendered.required_relations[0].sql == "DB.SCH.DOCS"


def test_profiles_carry_their_skills_and_plugins_so_agents_need_not_reference_them() -> None:
    shared = SharedProfile(None, (), ("shared-skill",), Origin("shared"))
    profile = DesktopProfile(
        "analyst", "profiles/analyst", "P.", "Data", ("solo",), (), (), None, Origin("p"), plugins=("kit",)
    )
    inputs = InMemoryProjectInputs(
        tree={"skills": CHANNELS},
        skills=SkillCatalog((skill("solo"), skill("shared-skill"), skill("lonely")), (plugin("kit", "solo"),)),
        profiles=ProfileCatalog((profile,), shared),
        agent_models=(agent("helper"),),
    )

    result = CompileProject(inputs).run()

    unreferenced = {str(item.subject) for item in result.diagnostics if item.code == "SST-VAL804"}
    assert unreferenced == {"skill:lonely"}


def test_a_stage_block_without_a_stage_compiles_profiles_without_the_desktop_channel() -> None:
    profile = DesktopProfile("analyst", "profiles/analyst", "P.", "Data", (), (), (), None, Origin("p"))
    inputs = InMemoryProjectInputs(
        tree={"skills": {"catalog": {"+bundle_stage": "OTHER.SCHEMA.BUNDLES"}, "stage": {"+registry_table": "R"}}},
        profiles=ProfileCatalog((profile,)),
        skills=SkillCatalog((skill("solo"),)),
        has_dbt_project=False,
    )

    result = CompileProject(inputs).run()

    assert [item.artifact_key for item in result.compiled] == ["skill:solo"]
    [extension] = result.compiled
    assert isinstance(extension, CompiledExtension)
    assert extension.release.stage.sql == "OTHER.SCHEMA.BUNDLES"


def test_unpublished_reasons_name_errors_first_then_the_channel_problem() -> None:
    catalog = with_broken_skill(plugin("kit", "draft"))
    channel = CatalogChannel("DB", "S", QualifiedName.parse("DB.S.BUNDLES"))
    compiled = CompileSkills(catalog, channel).run_result()

    assert unpublished_reasons(catalog, compiled, None) == {"skill:broken": "it has errors"}
    nothing = CompileResult((), compiled.diagnostics)
    assert unpublished_reasons(catalog, nothing, "no channel") == {
        "skill:draft": "no channel",
        "skill:broken": "it has errors",
        "plugin:kit": "no channel",
    }
    assert (
        unpublished_reasons(catalog, CompileResult(()), None)["plugin:kit"] == "the catalog channel cannot publish it"
    )


def test_extension_pins_map_each_skill_and_each_plugin_with_its_members() -> None:
    catalog = SkillCatalog((skill("first"), skill("second")), (plugin("kit", "first", "second"),))
    compiled = CompileSkills(catalog, CatalogChannel("DB", "S", QualifiedName.parse("DB.S.BUNDLES"))).run_result()
    other = CompileResult((object(),))  # type: ignore[arg-type]

    skills, plugins, members = extension_pins(CompileResult((*compiled.compiled, *other.compiled)))

    assert sorted(skills) == ["first", "second"] and skills["first"].members == ("first",)
    assert plugins["kit"].members == ("first", "second")
    assert members == frozenset(("skill:first", "skill:second"))


def test_merge_orders_by_type_position_then_key_and_keeps_diagnostic_order() -> None:
    catalog = SkillCatalog((skill("b-skill"), skill("a-skill")))
    skills = CompileSkills(catalog, CatalogChannel("DB", "S", QualifiedName.parse("DB.S.BUNDLES"))).run_result()
    first = CompileResult((), DiagnosticBag((D("SST-CFG003", key="x", origin=Origin("sst_config.yml")),)))

    merged = CompileArtifacts.merge((first, skills), POSITIONS)

    assert [item.artifact_key for item in merged.compiled] == ["skill:a-skill", "skill:b-skill"]
    assert merged.diagnostics[0].code == "SST-CFG003"
    assert isinstance(POSITIONS, MappingProxyType) and POSITIONS["semantic_view"] < POSITIONS["eval"]
