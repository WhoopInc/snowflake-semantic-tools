"""Cortex Code agent profiles in the domain: their values, how they render, and how they are checked.

Cortex Code agent profiles are a Snowflake private preview (PrPr) feature.
"""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Origin
from snowflake_semantic_tools.domain.diagnostics.specs.val_coco_profile import SPECS
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.coco_profile import (
    COCO_PROFILE_SCHEMA,
    REJECTED_COCO_PROFILE_KEYS,
    CocoProfileCatalog,
    ResolvedCocoProfile,
    listed_by_cortex_code,
    owned_entries,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.coco_profile import (
    assemble_instructions,
    desired_coco_profile_definition,
    render_coco_profile_json,
    render_coco_profile_spec,
)
from snowflake_semantic_tools.domain.validate.coco_profile import (
    coco_profile_diagnostics,
    default_profile_problems,
    schema_problems,
    unlisted_profile,
)
from tests.helpers.skill_inputs import profile, shared

LISTED = QualifiedName.parse("CORTEX_CODE.CONFIG.ANALYST")
REHEARSAL = QualifiedName.parse("DB.COCO_PROFILES.ANALYST")
AUTO = frozenset(("auto",))


def _pinned(name: str, path: str, version: str = "") -> AgentSkill:
    return AgentSkill(name, "CORTEX_EXTENSION", path, version, ref="skill")


def test_owned_entries_author_each_name_as_its_source_path() -> None:
    assert owned_entries("plugin", ("a", "b")) == (
        AgentSkill("", "CORTEX_EXTENSION", "a", "", ref="plugin"),
        AgentSkill("", "CORTEX_EXTENSION", "b", "", ref="plugin"),
    )
    assert owned_entries("skill", ()) == ()


def test_cortex_code_lists_only_profiles_in_its_config_schema_as_snowflake_resolves_it() -> None:
    assert COCO_PROFILE_SCHEMA == ("CORTEX_CODE", "CONFIG")
    assert listed_by_cortex_code(LISTED)
    assert listed_by_cortex_code(QualifiedName.parse("cortex_code.config.analyst"))
    assert not listed_by_cortex_code(REHEARSAL)
    assert not listed_by_cortex_code(QualifiedName.parse("CORTEX_CODE.OTHER.ANALYST"))


def test_a_profile_is_keyed_by_its_casefolded_name_and_a_catalog_keeps_folder_order() -> None:
    assert profile("Analyst").key == "coco_profile:analyst"
    catalog = CocoProfileCatalog((profile("b"), profile("a")), None)
    assert [item.name for item in catalog.profiles] == ["b", "a"]
    assert dict(profile().settings) == {}
    assert set(REJECTED_COCO_PROFILE_KEYS) == {"owner_team", "hooks", "mcp_servers", "commands", "allowed_roles"}


def test_instructions_join_the_shared_prompt_its_rules_by_name_then_the_profile_s() -> None:
    layer = shared(prompt="  Shared.\n", rules=(("b.md", "Rule B.\n"), ("a.md", "Rule A.\n")))
    assert assemble_instructions(layer, profile(prompt="Own.\n")) == "Shared.\n\nRule A.\n\nRule B.\n\nOwn.\n"
    # Blank or missing parts are dropped; nothing left is no instructions at all.
    assert assemble_instructions(shared(prompt=None, rules=(("a.md", "  \n"),)), profile(prompt=None)) == ""
    assert assemble_instructions(None, profile(prompt="Own.")) == "Own.\n"
    assert assemble_instructions(None, profile(prompt=None)) == ""


def test_a_spec_holds_only_what_the_profile_sets() -> None:
    skills = (_pinned("month-close", "DB.S.MONTH_CLOSE", "SST_0123456789AB"), _pinned("raw", "DB.S.RAW"))
    full = ResolvedCocoProfile(profile(orchestration_model="auto", skills=skills), "Be brief.\n")
    assert render_coco_profile_spec(full) == {
        "models": {"orchestration": "auto"},
        "instructions": {"response": "Be brief.\n"},
        "skills": [
            {
                "name": "month-close",
                "source": {"type": "CORTEX_EXTENSION", "path": "DB.S.MONTH_CLOSE", "version": "SST_0123456789AB"},
            },
            {"name": "raw", "source": {"type": "CORTEX_EXTENSION", "path": "DB.S.RAW"}},
        ],
    }
    empty = ResolvedCocoProfile(profile(skills=()), "")
    assert render_coco_profile_spec(empty) == {}
    assert json.loads(render_coco_profile_json(full)) == render_coco_profile_spec(full)


def test_a_profile_naming_only_a_model_renders_only_the_model() -> None:
    resolved = ResolvedCocoProfile(profile(orchestration_model="auto", prompt=None, skills=()), "")
    assert render_coco_profile_spec(resolved) == {"models": {"orchestration": "auto"}}


def test_the_definition_changes_with_the_comment_name_enabled_and_spec() -> None:
    model = profile()
    base = desired_coco_profile_definition(model, {"a": 1})
    assert json.loads(base) == {"comment": "Analyst.", "published_name": "analyst", "enabled": True, "spec": {"a": 1}}
    assert base == desired_coco_profile_definition(model, {"a": 1})
    for changed in (profile(comment="Other."), profile(published_name="analyst_dev"), profile(enabled=False)):
        assert desired_coco_profile_definition(changed, {"a": 1}) != base
    assert desired_coco_profile_definition(model, {"a": 2}) != base


def test_a_profile_s_model_must_be_allowed_and_an_unset_one_is_never_checked() -> None:
    assert coco_profile_diagnostics(profile(), None, AUTO) == ()
    assert coco_profile_diagnostics(profile(orchestration_model="auto"), None, AUTO) == ()
    [found] = coco_profile_diagnostics(profile(orchestration_model="other"), None, AUTO)
    assert (found.code, found.subject, found.context["found"]) == ("SST-VAL894", "coco_profile:analyst", "other")


def test_only_a_skill_the_shared_layer_lists_is_a_repeat() -> None:
    layer = shared(skills=("month-close", "finance-kit"))
    entries = (*owned_entries("skill", ("month-close", "own")), *owned_entries("plugin", ("finance-kit",)))
    found = coco_profile_diagnostics(profile(skills=entries), layer, AUTO)
    # A plugin of the shared layer's name is a different extension, so only the skill repeats.
    assert [(item.code, item.context["name"]) for item in found] == [("SST-VAL845", "month-close")]
    assert coco_profile_diagnostics(profile(), shared(), AUTO) == ()


def test_a_profile_outside_the_config_schema_is_a_rehearsal() -> None:
    assert unlisted_profile(profile(), LISTED) == ()
    [found] = unlisted_profile(profile(), REHEARSAL)
    assert (found.code, found.context["target"]) == ("SST-VAL891", "DB.COCO_PROFILES")


def test_several_listed_profiles_need_one_named_default() -> None:
    analyst, operator = profile("analyst"), profile("operator")
    operator_target = QualifiedName.parse("CORTEX_CODE.CONFIG.OPERATOR")
    [found] = default_profile_problems([(analyst, LISTED), (operator, operator_target)])
    assert (found.code, found.subject, found.origin) == ("SST-VAL893", None, Origin(analyst.source_files[0]))
    assert (found.context["count"], found.context["names"]) == (2, "ANALYST, OPERATOR")
    default = QualifiedName.parse("cortex_code.config.default")
    assert default_profile_problems([(analyst, LISTED), (operator, default)]) == ()
    # One listed profile applies to every role that sees it; a rehearsal is not listed at all.
    assert default_profile_problems([(analyst, LISTED), (operator, REHEARSAL)]) == ()
    assert default_profile_problems([]) == ()


def test_a_profile_may_not_share_a_schema_with_another_agent_object() -> None:
    others = [
        ("sales", QualifiedName.parse("DB.S.SALES")),
        ("ops", QualifiedName.parse("db.s.OPS")),
        ("everyone", QualifiedName.parse("SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE")),
    ]
    same = QualifiedName.parse("DB.S.ANALYST")
    found = schema_problems(others, [(profile("analyst"), same), (profile("operator"), LISTED)])
    # The first other object published to the schema is the one named.
    assert [(item.code, item.subject, item.context["other"]) for item in found] == [
        ("SST-VAL892", "coco_profile:analyst", "sales")
    ]
    assert schema_problems([], [(profile(), same)]) == ()


def test_every_cortex_code_profile_code_is_registered_in_the_890s() -> None:
    assert [item.code for item in SPECS] == [f"SST-VAL89{digit}" for digit in range(6)]
    assert all(ERROR_REGISTRY[item.code] is item for item in SPECS)
