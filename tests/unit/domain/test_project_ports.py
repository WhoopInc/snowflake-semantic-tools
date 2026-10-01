"""Config value readers, the project and golden ports, and whether a saved plan still applies."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.config_schema import (
    config_block,
    config_bool,
    config_int,
    config_text,
    configured_dir,
    skills_configured,
    target_text,
)
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.ports.golden import GoldenPath, GoldenStore
from snowflake_semantic_tools.domain.ports.project import ProjectInputs, ValidationDefaults
from snowflake_semantic_tools.domain.state import PlanMismatch, SavedChange, SavedPlan

TARGET = TargetIdentity("dev", "ACCT", Identifier.parse("db"), Identifier.parse("sch"), "ROLE", "WH")


def test_blocks_and_project_roots_read_as_empty_or_default_when_absent_or_mistyped() -> None:
    assert config_block({1: "one"}) == {"1": "one"}
    assert config_block(["not", "a", "block"]) == {}
    assert configured_dir({"project": {"tools_dir": "t"}}, "tools_dir", "tools") == "t"
    assert configured_dir({"project": {"tools_dir": ""}}, "tools_dir", "tools") == "tools"
    assert configured_dir({"project": "nope"}, "tools_dir", "tools") == "tools"
    assert [skills_configured({"skills": block}) for block in ({"catalog": {}}, {"stage": {}}, {"extensions": {}})] == [
        True,
        True,
        False,
    ]


def test_config_text_replaces_every_target_template_with_the_default() -> None:
    template = "{{ target.database }}.{{ target.schema }}.{{ target.warehouse }}"
    assert config_text(template, "X") == "X.X.X"
    assert config_text(None, "X") == "X"
    assert config_text(7, "X") == "7"
    assert config_text("{{ target.database }}", None) is None
    assert config_text("", "fallback") == "fallback"


def test_target_text_replaces_each_template_with_the_targets_own_value() -> None:
    template = "{{ target.database }}.{{ target.schema }}.{{ target.warehouse }}"
    assert target_text(template, TARGET, None) == "DB.SCH.WH"
    assert target_text("{{ target.warehouse }}", replace(TARGET, warehouse=None), "default") == "default"
    assert target_text(None, TARGET, "default") == "default"
    assert target_text(3, TARGET, "default") == "3"


def test_integers_and_flags_read_only_their_own_type() -> None:
    assert [config_int(value) for value in (4, True, "4", None)] == [4, None, None, None]
    assert [config_bool(value) for value in (False, 0, "true")] == [False, None, None]


def test_validation_flags_override_the_configured_defaults() -> None:
    defaults = ValidationDefaults(strict=True, snowflake_syntax_check=False)
    assert defaults.resolve(None, None) == (True, False)
    assert defaults.resolve(False, True) == (False, True)
    assert ValidationDefaults().resolve(None, None) == (False, True)


def test_project_and_golden_port_methods_are_declarations_only() -> None:
    source = object()
    assert ProjectInputs.config(source) is None  # type: ignore[arg-type]
    assert ProjectInputs.target(source) is None  # type: ignore[arg-type]
    assert ProjectInputs.skill_catalog(source, skills_dir="s", plugins_dir="p") is None  # type: ignore[arg-type]
    assert (
        ProjectInputs.profile_catalog(source, profiles_dir="p", hooks_dir="h", mcp_servers_dir="m", commands_dir="c")  # type: ignore[arg-type]
        is None
    )
    assert ProjectInputs.dbt_catalog(source) is None  # type: ignore[arg-type]
    assert ProjectInputs.tool_catalog(source) is None  # type: ignore[arg-type]
    assert ProjectInputs.agents(source, agents_dir="a") is None  # type: ignore[arg-type]
    assert ProjectInputs.eval_catalog(source, (), DiagnosticBag(), None) is None  # type: ignore[arg-type]
    assert ProjectInputs.git_sha(source) is None  # type: ignore[arg-type]
    assert ProjectInputs.validation_defaults(source) is None  # type: ignore[arg-type]
    assert ProjectInputs.manifest_sources(source) is None  # type: ignore[arg-type]
    path = GoldenPath("agent", ("a.json",))
    assert GoldenStore.exists(source, path) is None  # type: ignore[arg-type]
    assert GoldenStore.read(source, path) is None  # type: ignore[arg-type]
    assert GoldenStore.name(source, path) is None  # type: ignore[arg-type]


def change(**overrides: object) -> SavedChange:
    values: dict[str, object] = {
        "key": "semantic_view:v",
        "artifact_type": "semantic_view",
        "action": "create",
        "reason": "not_present",
        "target": "DB.SCH.V",
        "fingerprint": "f" * 64,
        "previous_marker": None,
        "statement_hashes": ("a" * 64,),
        "depends_on": (),
        "order": 100,
    }
    values.update(overrides)
    return SavedChange(**values)  # type: ignore[arg-type]


def plan(**overrides: object) -> SavedPlan:
    values: dict[str, object] = {
        "schema_version": 2,
        "plan_id": "p" * 64,
        "manifest_id": "m" * 64,
        "target": TARGET,
        "observation_at": "now",
        "observation_fingerprint": "o" * 64,
        "changes": (change(),),
    }
    values.update(overrides)
    return SavedPlan(**values)  # type: ignore[arg-type]


def test_a_saved_plan_applies_only_in_place_of_an_identical_fresh_plan() -> None:
    current = plan()
    assert plan().check_applicable(current, source="plan.json") is None


def test_a_saved_plan_for_another_target_names_both_targets() -> None:
    saved = plan(target=replace(TARGET, role="OTHER", warehouse=None))

    mismatch = saved.check_applicable(plan(), source="plan.json")

    assert isinstance(mismatch, PlanMismatch)
    [diagnostic] = mismatch.diagnostics
    assert diagnostic.code == "SST-APL005"
    assert mismatch.message == diagnostic.message
    assert (
        mismatch.message
        == "plan.json: plan target 'dev/ACCT/DB/SCH/OTHER' differs from apply target 'dev/ACCT/DB/SCH/ROLE/WH'"
    )


def test_a_stale_saved_plan_says_what_changed_in_order() -> None:
    current = plan()
    cases = [
        (plan(manifest_id="b" * 64), "the saved plan was made from a different manifest; re-run sst plan"),
        (plan(observation_fingerprint="0" * 64), "saved plan is stale; the live Snowflake observation changed"),
        (
            plan(changes=(change(statement_hashes=("1" * 64,)),)),
            "saved plan is stale; the ordered changes or statement hashes changed",
        ),
        (
            plan(manifest_id="b" * 64, observation_fingerprint="0" * 64),
            "the saved plan was made from a different manifest; re-run sst plan",
        ),
    ]

    found = [saved.check_applicable(current, source="plan.json") for saved, _ in cases]

    assert found == [PlanMismatch(message) for _, message in cases]
