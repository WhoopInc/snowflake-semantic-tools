"""Where each skill, plugin, and profile publishes, resolved per item through the one precedence chain.

A skill or plugin publishes where its `+bundle_stage` resolves, a profile where its `+database` and `+schema` do:
its own `config:`, then the closest folder route over its folder, then its block's root. So one
item can be placed or certified alone, and each reports the settings it took and where from.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.coco_profiles import CompiledCocoProfile
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.domain.model.coco_profile import CocoProfileModel
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog
from tests.helpers.compile_builders import plugin, skill
from tests.helpers.project_inputs import InMemoryProjectInputs, dev_target
from tests.helpers.skill_inputs import profile, profile_catalog


def _compiled(tree: dict[str, object], *skills: Skill, **inputs: object) -> CompileResult:
    catalog = inputs.pop("catalog", None) or SkillCatalog(skills)
    return CompileProject(
        InMemoryProjectInputs(tree=tree, skills=catalog, has_dbt_project=False, **inputs)  # type: ignore[arg-type]
    ).run()


def _extensions(result: CompileResult) -> dict[str, CompiledExtension]:
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def _profile(name: str, **settings: tuple[object, str]) -> CocoProfileModel:
    """A profile pinning nothing, its settings as loading resolved them: `key=(value, source)`."""
    resolved = {key: Setting(key, value, source) for key, (value, source) in settings.items()}
    return profile(name, skills=(), settings=MappingProxyType(resolved))


def _profiles(result: CompileResult) -> dict[str, CompiledCocoProfile]:
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledCocoProfile)}


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


def test_each_profile_publishes_where_its_own_settings_place_it_and_reports_where_from() -> None:
    profiles = profile_catalog(
        _profile("analyst", database=("CORTEX_CODE", "root"), schema=("CONFIG", "root")),
        _profile("operator", database=("CORTEX_CODE", "root"), schema=("OPS", "config")),
        _profile("x"),
    )
    compiled = _profiles(_compiled({}, profiles=profiles))

    assert {key: item.target.sql for key, item in compiled.items()} == {
        "coco_profile:analyst": "CORTEX_CODE.CONFIG.ANALYST",
        "coco_profile:operator": "CORTEX_CODE.OPS.OPERATOR",
        "coco_profile:x": "DB.SCH.X",
    }
    settings = {item.key: (item.value, item.source) for item in compiled["coco_profile:operator"].settings}
    assert settings == {
        "database": ("CORTEX_CODE", "root"),
        "enabled": (True, "default"),
        "name_suffix": ("", "default"),
        "schema": ("OPS", "config"),
    }


def test_a_disabled_profile_is_placed_but_does_not_publish() -> None:
    profiles = profile_catalog(_profile("analyst"), replace(_profile("operator"), enabled=False))
    assert list(_profiles(_compiled({}, profiles=profiles))) == ["coco_profile:analyst"]


def _unnamed_warehouse() -> TargetIdentity:
    """A target whose warehouse is no unquoted identifier, so `{{ target.warehouse }}` renders to none."""
    return replace(dev_target(), warehouse="my wh")


def test_a_location_that_renders_to_no_name_is_refused_and_the_targets_own_used() -> None:
    tree: dict[str, object] = {
        "skills": {"+bundle_stage": "{{ target.warehouse }}", "+schema": "{{ target.warehouse }}"},
        "agents": {"+database": "{{ target.warehouse }}"},
        "tools": {"+schema": "S"},
    }
    own = replace(skill("own"), config={"schema": "{{ target.warehouse }}"})
    inputs = InMemoryProjectInputs(tree=tree, skills=SkillCatalog((skill("close"), own)), identity=_unnamed_warehouse())
    result = CompileProject(inputs).run()
    refused = [(item.subject, item.context["key"]) for item in result.diagnostics if item.code == "SST-CFG008"]
    # Each skill reports the root's bundle stage, which leaves it nowhere to publish; the
    # schema of its own `config:` names the skill, the root's this file's key.
    assert refused == [
        ("config:skills.+bundle_stage", "skills.+bundle_stage"),
        ("config:skills.+bundle_stage", "skills.+bundle_stage"),
        ("config:agents.+database", "agents.+database"),
    ]
    assert _extensions(result) == {}
    placed = {**tree, "skills": {"+bundle_stage": "BUNDLES", "+schema": "{{ target.warehouse }}"}}
    result = CompileProject(replace(inputs, tree=placed)).run()
    refused = [(item.subject, item.context["key"]) for item in result.diagnostics if item.code == "SST-CFG008"]
    assert ("skill:own", "config.schema") in refused and ("config:skills.+schema", "skills.+schema") in refused
    assert {item.release.target.schema.folded for item in _extensions(result).values()} == {"SCH"}


def test_a_location_already_refused_where_it_was_written_is_not_reported_again() -> None:
    # The value check of the file or the item's `config:` reported it; this one only steps around it.
    own = replace(skill("own"), config={"schema": " "})
    tree: dict[str, object] = {"skills": {"+bundle_stage": "BUNDLES"}, "agents": {"+schema": "A B"}}
    result = CompileProject(InMemoryProjectInputs(tree=tree, skills=SkillCatalog((own,)))).run()
    assert "SST-CFG008" not in [item.code for item in result.diagnostics]
    assert [item.release.target.schema.folded for item in _extensions(result).values()] == ["SCH"]


def test_a_profile_location_or_suffix_that_renders_to_no_name_is_refused_once() -> None:
    profiles = profile_catalog(
        _profile("analyst", schema=("{{ target.warehouse }}", "route:ops")),
        _profile("operator", database=("{{ target.warehouse }}", "config"), name_suffix=("bad suffix!", "config")),
    )
    result = _compiled({}, profiles=profiles, identity=_unnamed_warehouse())

    refused = [(item.subject, item.context["key"]) for item in result.diagnostics if item.code == "SST-CFG008"]
    assert refused == [
        ("config:coco_profiles.ops.+schema", "coco_profiles.ops.+schema"),
        ("coco_profile:operator", "config.database"),
        ("coco_profile:operator", "config.name_suffix"),
    ]
    # The target's own location stands in, and no suffix is used.
    assert {key: item.target.sql for key, item in _profiles(result).items()} == {
        "coco_profile:analyst": "DB.SCH.ANALYST",
        "coco_profile:operator": "DB.SCH.OPERATOR",
    }
