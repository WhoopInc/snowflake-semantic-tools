"""Where each skill, plugin, and profile publishes, resolved per item through the one precedence chain.

A skill or plugin publishes where its `+bundle_stage` resolves, a profile where its `+stage` does:
its own `config:`, then the closest folder route over its folder, then its block's root. So one
item can be placed or certified alone, and each reports the settings it took and where from.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.profile import DesktopProfile, ProfileCatalog
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog
from tests.helpers.compile_builders import plugin, skill
from tests.helpers.project_inputs import InMemoryProjectInputs


def _compiled(tree: dict[str, object], *skills: Skill, **inputs: object) -> CompileResult:
    catalog = inputs.pop("catalog", None) or SkillCatalog(skills)
    return CompileProject(
        InMemoryProjectInputs(tree=tree, skills=catalog, has_dbt_project=False, **inputs)  # type: ignore[arg-type]
    ).run()


def _extensions(result: CompileResult) -> dict[str, CompiledExtension]:
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def _profile(name: str, **config: object) -> DesktopProfile:
    return DesktopProfile(name, f"profiles/{name}", "P.", "Data", (), (), (), None, Origin("p"), config=config)


def test_one_skill_certifies_and_places_itself_while_the_others_take_the_root() -> None:
    tree: dict[str, object] = {"skills": {"+bundle_stage": "BUNDLES", "+schema": "SKILLS"}}
    own = replace(skill("close"), config={"certified": True, "schema": "FINANCE"})
    found = _extensions(_compiled(tree, own, skill("triage")))

    assert found["skill:close"].release.certified and not found["skill:triage"].release.certified
    assert found["skill:close"].release.target.sql == "DB.FINANCE.CLOSE"
    assert found["skill:triage"].release.target.sql == "DB.SKILLS.TRIAGE"
    # An unqualified bundle stage sits beside each extension, in the location its settings give.
    assert [found[key].release.stage.sql for key in ("skill:close", "skill:triage")] == [
        "DB.FINANCE.BUNDLES",
        "DB.SKILLS.BUNDLES",
    ]


def test_a_route_over_a_skills_folder_beats_the_root_and_its_own_config_beats_the_route() -> None:
    tree: dict[str, object] = {"skills": {"+bundle_stage": "B", "finance": {"+schema": "FIN", "+certified": True}}}
    routed = replace(skill("close"), folder=("finance", "close"))
    own = replace(skill("audit"), folder=("finance", "audit"), config={"certified": False})
    found = _extensions(_compiled(tree, routed, own, skill("top")))

    assert [found[key].release.target.schema.folded for key in ("skill:close", "skill:audit", "skill:top")] == [
        "FIN",
        "FIN",
        "SCH",
    ]
    assert (found["skill:close"].release.certified, found["skill:audit"].release.certified) == (True, False)
    settings = {item.key: (item.value, item.source) for item in found["skill:audit"].settings}
    assert settings == {
        "bundle_stage": ("B", "root"),
        "certified": (False, "config"),
        "database": ("DB", "default"),
        "schema": ("FIN", "route:finance"),
        "version_prefix": ("SST_", "default"),
    }


def test_a_skill_placed_outside_its_roots_schema_is_reported_and_still_publishes() -> None:
    tree: dict[str, object] = {"skills": {"+bundle_stage": "B"}}
    moved = replace(skill("close"), config={"schema": "ELSEWHERE"})
    result = _compiled(tree, moved, skill("stays"))

    split = [item for item in result.diagnostics if item.code == "SST-VAL806"]
    assert [(item.subject, item.context["value"]) for item in split] == [("skill:close", "DB.ELSEWHERE")]
    assert set(_extensions(result)) == {"skill:close", "skill:stays"}


def test_a_version_prefix_no_alias_can_start_is_reported_once_where_it_is_written() -> None:
    root = _compiled({"skills": {"+bundle_stage": "B", "+version_prefix": "1"}}, skill("a"), skill("b"))
    [shared] = [item for item in root.diagnostics if item.code == "SST-CFG008"]
    assert (shared.subject, shared.context["key"]) == ("config:skills.+version_prefix", "skills.+version_prefix")
    assert _extensions(root) == {}

    routed = replace(skill("a"), folder=("ops", "a"))
    route = _compiled({"skills": {"+bundle_stage": "B", "ops": {"+version_prefix": "9"}}}, routed, skill("b"))
    [placed] = [item for item in route.diagnostics if item.code == "SST-CFG008"]
    assert placed.subject == "config:skills.ops.+version_prefix"
    assert set(_extensions(route)) == {"skill:b"}

    own = replace(skill("a"), config={"version_prefix": "-"})
    item_result = _compiled({"skills": {"+bundle_stage": "B"}}, own, skill("b"))
    [mine] = [item for item in item_result.diagnostics if item.code == "SST-CFG008"]
    assert (mine.subject, mine.context["key"], mine.origin) == ("skill:a", "config.version_prefix", own.origin)


def test_a_plugin_publishes_from_its_own_block_whatever_its_members_do() -> None:
    catalog = SkillCatalog((skill("close"),), (plugin("kit", "close"),))
    # Only plugins: publishes; the member is bundled into the plugin without publishing alone.
    alone = _extensions(_compiled({"plugins": {"+bundle_stage": "B", "+version_prefix": "KIT_"}}, catalog=catalog))
    assert set(alone) == {"plugin:kit"}
    assert alone["plugin:kit"].release.alias.startswith("KIT_")
    # And a skills: root alone publishes no plugin.
    assert set(_extensions(_compiled({"skills": {"+bundle_stage": "B"}}, catalog=catalog))) == {"skill:close"}


def test_each_profile_publishes_through_its_own_stage_and_each_registry_is_noted_once() -> None:
    profiles = ProfileCatalog((_profile("analyst"), _profile("operator", schema="OPS"), _profile("x")))
    tree: dict[str, object] = {"profiles": {"+stage": "TREES", "x": {"+registry_table": "OTHER"}}}
    result = _compiled(tree, profiles=profiles)

    compiled = {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledProfile)}
    assert {key: item.channel.stage.sql for key, item in compiled.items()} == {
        "profile:analyst": "DB.SCH.TREES",
        "profile:operator": "DB.OPS.TREES",
        "profile:x": "DB.SCH.TREES",
    }
    assert compiled["profile:x"].channel.registry.sql == "DB.SCH.OTHER"
    notes = [item.context["value"] for item in result.diagnostics if item.code == "SST-VAL854"]
    assert notes == ["DB.SCH.PROFILE_REGISTRY", "DB.OPS.PROFILE_REGISTRY", "DB.SCH.OTHER"]
    settings = {item.key: (item.value, item.source) for item in compiled["profile:x"].settings}
    assert settings["registry_table"] == ("OTHER", "route:x")


def test_a_profile_no_stage_reaches_is_checked_and_does_not_publish() -> None:
    profiles = ProfileCatalog((_profile("analyst"), _profile("operator", stage="TREES")))
    result = _compiled({"profiles": {"+registry_table": "R"}}, profiles=profiles)
    assert [item.artifact_key for item in result.compiled] == ["profile:operator"]


def test_a_root_only_setting_is_resolved_but_never_reported_by_an_item() -> None:
    found = _extensions(_compiled({"skills": {"+bundle_stage": "B", "+threads": 2}}, skill("close")))
    assert "threads" not in {item.key for item in found["skill:close"].settings}
