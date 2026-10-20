"""The whole-project compile, driven by an in-memory `ProjectInputs`.

What it reads, in what order, and what it builds.
"""

from __future__ import annotations

import json
from dataclasses import replace
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
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill, AgentTool
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.profile import DesktopProfile, ProfileCatalog, SharedProfile
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.skill import Plugin, SkillCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
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


def test_a_project_without_dbt_reads_only_its_configuration_and_skill_and_profile_catalogs() -> None:
    inputs = InMemoryProjectInputs(
        has_dbt_project=False,
        config_diagnostics=DiagnosticBag((D("SST-CFG003", key="x", origin=Origin("sst_config.yml")),)),
    )

    result = CompileProject(inputs).run()

    assert inputs.reads == [
        "config",
        "target",
        "skill_catalog:skills,plugins",
        "profile_catalog:profiles,hooks,mcp-servers,commands",
    ]
    assert result.compiled == ()
    assert [item.code for item in result.diagnostics] == ["SST-CFG003"]


def test_a_dbt_project_is_read_in_a_fixed_order_from_its_configured_roots() -> None:
    inputs = InMemoryProjectInputs(
        tree={
            "project": {"skills_dir": "sk", "agents_dir": "ag", "hooks_dir": "hk", "commands_dir": "cm"},
            **CHANNELS,
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
        "cowork_profiles:cowork_profiles",
        "git_sha",
        "eval_catalog",
    ]


def test_results_merge_in_ddl_order_after_the_configuration_and_target_diagnostics() -> None:
    profile = DesktopProfile("analyst", "profiles/analyst", "P.", "Data", ("month-close",), (), (), None, Origin("p"))
    inputs = InMemoryProjectInputs(
        tree=dict(CHANNELS),
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
    def compile_with(tree: dict[str, object], skills: SkillCatalog | None = None) -> dict[str, str]:
        inputs = InMemoryProjectInputs(
            tree=tree,
            skills=skills or with_broken_skill(),
            agent_models=(agent("router", "draft", "broken"),),
        )
        return reasons(CompileProject(inputs).run())

    missing = "no +bundle_stage resolves for it"
    assert compile_with({}) == {"draft": missing, "broken": missing}
    assert compile_with({"profiles": {"+stage": "P"}}) == {"draft": missing, "broken": "it has errors"}
    assert compile_with({"skills": {}}) == {"draft": missing, "broken": "it has errors"}
    assert compile_with({"skills": {"+bundle_stage": "B"}}) == {"broken": "it has errors"}
    bad_prefix = compile_with({"skills": {"+version_prefix": "1", "+bundle_stage": "B"}})
    assert bad_prefix["draft"] == "the catalog channel cannot publish it"
    # A route over the skill's folder, or its own `config:`, places it with no block root at all.
    base = with_broken_skill()
    draft, broken = base.skills
    routed = replace(base, skills=(replace(draft, folder=("ops", "draft")), broken))
    assert compile_with({"skills": {"ops": {"+bundle_stage": "B"}}}, routed) == {"broken": "it has errors"}
    own = replace(base, skills=(replace(draft, config={"bundle_stage": "B"}), broken))
    assert compile_with({}, own) == {"broken": "it has errors"}


def test_an_extension_entry_that_cannot_be_qualified_is_unpublished_for_that_reason() -> None:
    inputs = InMemoryProjectInputs(
        tree={"extensions": {"glossary": None}},
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

    assert reasons(result) == {"glossary": "its extensions: entry cannot be qualified"}
    assert result.diagnostics[0].code == "SST-CFG036"


def test_consumed_extensions_resolve_from_fqn_or_default_prefix() -> None:
    config: dict[str, object] = {
        "extensions": {
            "default_prefix": "{{ target.database }}.SHARED",
            "vendor-pack": None,
            "pinned": {"fqn": "OTHER.EXT.PINNED"},
            "dotted.name": None,
            "broken": {"fqn": "not a name"},
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
    without_prefix, diagnostics = consumed_extensions({"extensions": {"x": None}}, dev_target())
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
    assert isinstance(agents, tuple) and [model.name for model in agents] == ["helper"]
    assert tool_names == {"helper": ()}
    assert "SST-VAL543" not in {item.code for item in result.diagnostics}

    # Loading resolves `agents.+enabled` into each agent's `enabled`, so compiling trusts the
    # agent's own value: one that sets `enabled: true` compiles under a disabling default.
    kept = InMemoryProjectInputs(tree={"agents": {"+enabled": False}}, agent_models=(agent("helper"),))
    assert [item.artifact_type for item in CompileProject(kept).run().compiled] == ["agent"]
    requested = kept.eval_requests[0][0]
    assert isinstance(requested, tuple) and [model.name for model in requested] == ["helper"]


def test_only_auto_is_allowed_without_a_list_of_orchestration_models() -> None:
    inputs = InMemoryProjectInputs(agent_models=(agent("helper", model="fast"),))

    result = CompileProject(inputs).run()

    assert "SST-VAL543" in {item.code for item in result.diagnostics}


def test_tools_take_the_tools_block_defaults_and_resolve_dbt_models_to_relations() -> None:
    search = ToolMember(
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
        tools=ToolCatalog((search,), "dev", frozenset(("dev",))),
        dbt=DbtCatalog("v12", None, None, (DbtModel("model.docs", "docs", "DB.SCH.DOCS", ("ID",), (), ()),)),
    )

    rendered = CompileProject(inputs).run().rendered[0]

    assert rendered.target.sql == "DB.TOOLS.SEARCH"
    assert "WAREHOUSE = WH_XL" in rendered.ddl
    assert "TARGET_LAG = '1 hour'" in rendered.ddl
    assert rendered.required_relations[0].sql == "DB.SCH.DOCS"


def test_each_tool_member_resolves_its_own_settings_over_its_folder_routes() -> None:
    def search(name: str, folder: tuple[str, ...], **config: object) -> ToolMember:
        return ToolMember(
            name,
            "cortex_search_service",
            ToolOwnership.DEFINE,
            Origin("t"),
            "t",
            on_model="docs",
            search_column="body",
            folder=folder,
            config=MappingProxyType(config),
        )

    tree = {
        "tools": {
            "+schema": "TOOLS",
            "+target_lag": "1 hour",
            "platform": {"+schema": "PLATFORM", "docs": {"+warehouse": "DOCS_WH"}},
        }
    }
    members = (search("root", ()), search("nested", ("platform", "docs")), search("own", ("platform",), schema="OWN"))
    inputs = InMemoryProjectInputs(
        tree=tree,
        tools=ToolCatalog(members, "dev", frozenset(("dev",))),
        dbt=DbtCatalog("v12", None, None, (DbtModel("model.docs", "docs", "DB.SCH.DOCS", ("ID",), (), ()),)),
    )

    compiled = {item.name: item for item in CompileProject(inputs).run().compiled if item.artifact_type == "tool"}

    assert {name: item.rendered_artifact.target.sql for name, item in compiled.items()} == {
        "root": "DB.TOOLS.ROOT",
        "nested": "DB.PLATFORM.NESTED",
        "own": "DB.OWN.OWN",
    }
    assert "WAREHOUSE = DOCS_WH" in compiled["nested"].rendered_artifact.ddl
    assert {item.key: item.source for item in compiled["nested"].settings} == {
        "database": "default",
        "execute_as": "default",
        "immutable": "default",
        "schema": "route:platform",
        "target_lag": "root",
        "warehouse": "route:platform/docs",
    }
    assert {item.key: item.source for item in compiled["own"].settings}["schema"] == "config"


def test_profiles_carry_their_skills_and_plugins_so_agents_need_not_reference_them() -> None:
    shared = SharedProfile(None, (), ("shared-skill",), Origin("shared"))
    profile = DesktopProfile(
        "analyst", "profiles/analyst", "P.", "Data", ("solo",), (), (), None, Origin("p"), plugins=("kit",)
    )
    inputs = InMemoryProjectInputs(
        tree=dict(CHANNELS),
        skills=SkillCatalog((skill("solo"), skill("shared-skill"), skill("lonely")), (plugin("kit", "solo"),)),
        profiles=ProfileCatalog((profile,), shared),
        agent_models=(agent("helper"),),
    )

    result = CompileProject(inputs).run()

    unreferenced = {str(item.subject) for item in result.diagnostics if item.code == "SST-VAL804"}
    assert unreferenced == {"skill:lonely"}


def test_a_profiles_block_without_a_stage_compiles_no_profile() -> None:
    profile = DesktopProfile("analyst", "profiles/analyst", "P.", "Data", (), (), (), None, Origin("p"))
    inputs = InMemoryProjectInputs(
        tree={"skills": {"+bundle_stage": "OTHER.SCHEMA.BUNDLES"}, "profiles": {"+registry_table": "R"}},
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
    placed = frozenset(("skill:draft", "skill:broken", "plugin:kit"))

    assert unpublished_reasons(catalog, compiled, placed) == {"skill:broken": "it has errors"}
    nothing = CompileResult((), compiled.diagnostics)
    assert unpublished_reasons(catalog, nothing, frozenset()) == {
        "skill:draft": "no +bundle_stage resolves for it",
        "skill:broken": "it has errors",
        "plugin:kit": "no +bundle_stage resolves for it",
    }
    assert (
        unpublished_reasons(catalog, CompileResult(()), placed)["plugin:kit"] == "the catalog channel cannot publish it"
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


def test_a_tool_over_a_defined_member_calls_it_where_the_tools_block_publishes_it() -> None:
    search = ToolMember("search", "cortex_search_service", ToolOwnership.DEFINE, Origin("t"), "t")
    lookup = ToolMember("lookup", "procedure", ToolOwnership.DEFINE, Origin("t"), "t", warehouse="WH", folder=("ops",))
    catalog = ToolCatalog((search, lookup), "dev", frozenset(("dev",)))
    tools = (
        AgentTool("cortex_search", Origin("a"), name="search", description="Docs.", backing="search"),
        AgentTool(
            "generic",
            Origin("a"),
            name="lookup",
            description="Look one up.",
            backing="lookup",
            input_schema=MappingProxyType({"type": "object", "properties": {}}),
        ),
    )
    inputs = InMemoryProjectInputs(
        tree={"agents": {"+schema": "BOTS"}, "tools": {"+schema": "TOOLS", "ops": {"+schema": "OPS"}}},
        tools=catalog,
        agent_models=(AgentModel("helper", Origin("agent.yml"), ("agent.yml",), tools=tools),),
    )

    [compiled] = [item for item in CompileProject(inputs).run().compiled if isinstance(item, CompiledAgent)]

    resources = json.loads(compiled.payload)["tool_resources"]
    assert compiled.rendered_artifact.target.sql == "DB.BOTS.HELPER"
    assert resources["search"]["search_service"] == "DB.TOOLS.SEARCH"
    assert resources["lookup"]["identifier"] == "DB.OPS.LOOKUP"


def _consuming(tree: dict[str, object]) -> CompileResult:
    pinned = AgentSkill("glossary", "CORTEX_EXTENSION", "glossary", "", version_var="glossary_version")
    model = AgentModel("helper", Origin("agent.yml"), ("agent.yml",), skills=(pinned,))
    extensions = {"extensions": {"default_prefix": "DB.EXT", "glossary": None}}
    return CompileProject(InMemoryProjectInputs(tree={**extensions, **tree}, agent_models=(model,))).run()


def test_a_var_version_pins_the_value_vars_declares_and_an_undeclared_one_pins_none() -> None:
    declared = _consuming({"vars": {"glossary_version": "VERSION$3"}})

    assert "SST-VAL538" not in {item.code for item in declared.diagnostics}
    [compiled] = [item for item in declared.compiled if isinstance(item, CompiledAgent)]
    [entry] = json.loads(compiled.payload)["skills"]
    assert entry["source"]["path"] == "DB.EXT.GLOSSARY"
    assert entry["source"]["version"] == "VERSION$3"
    unresolved: tuple[dict[str, object], ...] = (
        {},
        {"vars": {"glossary_version": None}},
        {"vars": {"other": "VERSION$3"}},
    )
    for tree in unresolved:
        assert "SST-VAL538" in {item.code for item in _consuming(tree).diagnostics}
