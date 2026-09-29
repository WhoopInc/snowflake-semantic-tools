"""`sst_config.yml` keys are declared once and every deviation is diagnosed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_KEYS,
    CONFIG_SCHEMA,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    KeyKind,
    KeyStatus,
    validate_config,
)
from snowflake_semantic_tools.domain.model.diagnostic import ERROR_REGISTRY, Origin, Severity


def _codes(tree: dict[str, object]) -> list[tuple[str, str | None]]:
    return [(item.code, item.subject) for item in validate_config(tree)]


def test_schema_rows_are_unique_registered_and_self_describing() -> None:
    assert len(CONFIG_KEYS) == len(CONFIG_SCHEMA)
    assert TOP_LEVEL_KEYS[0] == "project"
    for key in CONFIG_SCHEMA:
        assert key.summary
        assert key.segments[-1] == key.name
        assert ".".join((*key.segments[:-1], key.name)) == key.path
        if key.code is not None:
            assert key.code in ERROR_REGISTRY
        if key.status is KeyStatus.DEPRECATED:
            assert key.replacement in CONFIG_KEYS
    assert CONFIG_KEYS["skills.catalog.+bundle_stage"].parent == "skills.catalog"
    assert CONFIG_KEYS["skills"].children is ChildPolicy.ROUTES


def test_a_complete_current_configuration_is_clean() -> None:
    tree: dict[str, object] = {
        "project": {"semantic_models_dir": "semantic_models", "skills_dir": "skills", "target_profile": "p"},
        "validation": {"strict": False, "snowflake_syntax_check": True},
        "vars": {"state": "done", "limits": {"low": 1}, "count": 3},
        "state": {"+table": "SST_STATE"},
        "tags": {"default_prefix": "DB.GOV", "pii": None, "tier": {"fqn": "DB.GOV.TIER"}},
        "tools": {"+execute_as": "owner"},
        "semantic_views": {"+schema": "SV", "+enabled": True, "core": {"+schema": "CORE", "deep": {"+schema": "D"}}},
        "agents": {"+query_timeout": 60, "+tool_not_accessible": "accept"},
        "evals": {"+retry": 1, "+eval_tier": "report"},
        "skills": {
            "+certified": False,
            "+threads": 16,
            "extensions": {"default_prefix": "DB.EXT", "consumed": None},
            "catalog": {"+bundle_stage": "SKILL_BUNDLE_SRC", "+flatten": True},
            "stage": {"+stage": "SKILL_BUNDLES", "+flatten": False, "+auto_compress": False, "+layout": "by_type"},
        },
        "apply": {"agent_spec_stage": {"stage": "AGENT_SPECS"}},
        "snowflake": {"orchestration_models": ["auto"]},
    }
    assert validate_config(tree) == ()


def test_unknown_keys_warn_and_top_level_near_misses_error() -> None:
    assert _codes({"skils": {}}) == [("SST-CFG007", "config:skils")]
    diagnostic = validate_config({"skils": {}})[0]
    assert "did you mean 'skills'" in diagnostic.message
    assert _codes({"entirely_new": 1}) == [("SST-CFG003", "config:entirely_new")]
    assert _codes({"project": {"skill_dir": "x"}}) == [("SST-CFG003", "config:project.skill_dir")]
    assert validate_config({"project": {"skill_dir": "x"}})[0].severity is Severity.WARNING


def test_positions_become_origins_when_known() -> None:
    located = validate_config({"project": {"nope": 1}}, positions={("project", "nope"): (4, 3)})
    assert located[0].origin == Origin("sst_config.yml", 4, 3)
    assert validate_config({"project": {"nope": 1}})[0].origin == Origin("sst_config.yml")


def test_removed_keys_carry_a_reason_or_a_dedicated_code() -> None:
    assert _codes({"skills": {"+grant_read_to": [], "stage": {"+stage": "S"}}}) == [
        ("SST-CFG043", "config:skills.+grant_read_to")
    ]
    message = validate_config({"skills": {"+grant_read_to": [], "stage": {"+stage": "S"}}})[0].message
    assert "managed outside SST" in message
    assert _codes({"vars": {"sha_version": "abc"}}) == [("SST-CFG040", "config:vars.sha_version")]
    assert _codes({"evals": {"+database": "X"}}) == [("SST-CFG015", "config:evals.+database")]
    assert _codes({"evals": {"nightly": {}}}) == [("SST-CFG042", "config:evals.nightly")]
    assert _codes({"skills": {"finance": {}, "catalog": {"+bundle_stage": "S"}}}) == [
        ("SST-CFG042", "config:skills.finance")
    ]


def test_inert_blocks_inform_once_and_are_not_descended() -> None:
    diagnostics = validate_config({"enrichment": {"distinct_limit": 25, "anything": True}})
    assert [(item.code, item.severity) for item in diagnostics] == [("SST-CFG044", Severity.INFO)]
    assert _codes({"agents": {"finance": {"+schema": "X"}}}) == [("SST-CFG044", "config:agents.finance")]
    assert _codes({"validation": {"exclude_dirs": []}}) == [("SST-CFG044", "config:validation.exclude_dirs")]


def test_deprecated_deploy_is_validated_as_apply() -> None:
    assert _codes({"deploy": {"agent_spec_stage": {"stage": "S"}}}) == [("SST-CFG045", "config:deploy")]
    assert _codes({"deploy": {"bogus": 1}}) == [
        ("SST-CFG045", "config:deploy"),
        ("SST-CFG003", "config:deploy.bogus"),
    ]


def test_types_domains_bounds_and_fixed_values() -> None:
    assert _codes({"project": {"skills_dir": 3}}) == [("SST-CFG004", "config:project.skills_dir")]
    assert _codes({"agents": {"+query_timeout": True}}) == [("SST-CFG004", "config:agents.+query_timeout")]
    assert _codes({"tags": {"pii": "not-a-block"}}) == [("SST-CFG004", "config:tags.pii")]
    assert _codes({"semantic_views": {"core": "flat"}}) == [("SST-CFG004", "config:semantic_views.core")]
    assert _codes({"vars": []}) == [("SST-CFG004", "config:vars")]
    assert _codes({"tools": {"+execute_as": "sudo"}}) == [("SST-CFG008", "config:tools.+execute_as")]
    stage = {"+stage": "S"}
    assert _codes({"skills": {"stage": {**stage, "+layout": "by_profile"}}}) == [
        ("SST-VAL821", "config:skills.stage.+layout")
    ]
    assert _codes({"skills": {"catalog": {"+bundle_stage": "S", "+flatten": False}}}) == [
        ("SST-VAL818", "config:skills.catalog.+flatten")
    ]
    assert _codes({"skills": {"stage": {**stage, "+flatten": True}}}) == [
        ("SST-VAL818", "config:skills.stage.+flatten")
    ]
    assert _codes({"skills": {"stage": {**stage, "+auto_compress": True}}}) == [
        ("SST-VAL819", "config:skills.stage.+auto_compress")
    ]
    for threads in (0, 17):
        assert _codes({"skills": {"+threads": threads, "stage": stage}}) == [("SST-CFG008", "config:skills.+threads")]
    assert "1..16" in validate_config({"skills": {"+threads": 0, "stage": stage}})[0].message


def test_required_keys_and_one_of_channel_blocks() -> None:
    assert _codes({"skills": {"catalog": {}}}) == [("SST-CFG006", "config:skills.catalog.+bundle_stage")]
    assert _codes({"skills": {"+certified": True}}) == [("SST-VAL817", "config:skills")]
    assert _codes({"skills": {"extensions": {"x": {"fqn": "A.B.C", "other": 1}}, "stage": {"+stage": "S"}}}) == [
        ("SST-CFG003", "config:skills.extensions.x.other")
    ]


def test_empty_values_mean_unset() -> None:
    assert _codes({"agents": {"+warehouse": None}, "tags": {"pii": None}}) == []
    assert _codes({"skills": None}) == [("SST-VAL817", "config:skills")]
    assert _codes({"skills": {"catalog": {"+bundle_stage": None}}}) == [
        ("SST-CFG006", "config:skills.catalog.+bundle_stage")
    ]


def test_every_kind_has_a_representative_key() -> None:
    kinds = {key.kind for key in CONFIG_SCHEMA}
    assert kinds == set(KeyKind)
