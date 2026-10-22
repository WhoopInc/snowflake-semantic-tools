"""`sst_config.yml` keys are declared once and every deviation is diagnosed."""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Origin, Severity
from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_KEYS,
    CONFIG_SCHEMA,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    KeyKind,
    KeyStatus,
)
from snowflake_semantic_tools.domain.model.tool import EXECUTE_AS
from snowflake_semantic_tools.domain.validate.config import unusable_name, validate_config, validated_config


def _codes(tree: Mapping[str, object]) -> list[tuple[str, str | None]]:
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
    assert {key.status for key in CONFIG_SCHEMA} == set(KeyStatus)
    assert CONFIG_KEYS["extensions.<name>.fqn"].parent == "extensions.<name>"
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
        "evals": {"+retry": 1, "+eval_tier": "report", "ops": {"+eval_tier": "blocking"}},
        "skills": {
            "+certified": False,
            "+threads": 16,
            "+bundle_stage": "SKILL_BUNDLE_SRC",
            "finance": {"+schema": "FINANCE", "close": {"+certified": True}},
        },
        "plugins": {"+bundle_stage": "SKILL_BUNDLE_SRC", "kits": {"+version_prefix": "KIT_"}},
        "coco_desktop_profiles": {
            "+stage": "SKILL_BUNDLES",
            "+registry_table": "PROFILE_REGISTRY",
            "analyst": {"+schema": "A"},
        },
        "extensions": {"default_prefix": "DB.EXT", "consumed": None},
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
    assert _codes({"skills": {"+grant_read_to": [], "+bundle_stage": "S"}}) == [
        ("SST-CFG043", "config:skills.+grant_read_to")
    ]
    message = validate_config({"skills": {"+grant_read_to": [], "+bundle_stage": "S"}})[0].message
    assert "managed outside SST" in message
    assert _codes({"vars": {"sha_version": "abc"}}) == [("SST-CFG040", "config:vars.sha_version")]
    assert _codes({"evals": {"+database": "X"}}) == [("SST-CFG015", "config:evals.+database")]
    assert _codes({"evals": {"nightly": {}}}) == []
    # A block that moved is reported where it was written, naming where it went.
    moved = validate_config({"skills": {"finance": {}, "catalog": {"+bundle_stage": "S"}, "stage": {}}})
    assert [(item.code, item.subject) for item in moved] == [
        ("SST-CFG042", "config:skills.catalog"),
        ("SST-CFG042", "config:skills.stage"),
    ]
    assert moved[0].message == (
        "skills: 'catalog' is no longer read here; set +bundle_stage, +database and +schema on skills: and "
        "plugins: themselves"
    )
    assert _codes({"skills": {"extensions": {"x": None}}}) == [("SST-CFG042", "config:skills.extensions")]


def test_agent_folder_routes_are_checked_as_the_agents_block_at_every_depth() -> None:
    assert _codes({"agents": {"finance": {"+schema": "X", "restricted": {"+secure": True}}}}) == []
    assert _codes({"agents": {"finance": {"+schema": 1}}}) == [("SST-CFG004", "config:agents.finance.+schema")]
    assert _codes({"agents": {"finance": {"restricted": {"+bogus": 1}}}}) == [
        ("SST-CFG003", "config:agents.finance.restricted.+bogus")
    ]
    assert _codes({"agents": {"finance": {"+meta": {}}}}) == [("SST-CFG043", "config:agents.finance.+meta")]
    assert _codes({"dbt": {"invoke": True, "anything": True}}) == [("SST-CFG003", "config:dbt.anything")]
    assert _codes({"tools": {"finance": {"+bogus": 1}}}) == [("SST-CFG003", "config:tools.finance.+bogus")]


def test_routed_models_meet_the_allowlist_and_every_publishing_block_takes_routes() -> None:
    tree = {"agents": {"+orchestration_model": "auto", "finance": {"core": {"+orchestration_model": "claude-x"}}}}
    assert _codes(tree) == [("SST-CFG025", "config:agents.finance.core.+orchestration_model")]
    assert _codes({**tree, "snowflake": {"orchestration_models": ["auto", "claude-x"]}}) == []
    assert _codes({"evals": {"finance": {"+retry": 1}}}) == []
    assert _codes({"skills": {"finance": {"+bundle_stage": "S"}}}) == []
    assert _codes({"coco_desktop_profiles": {"analyst": {"+stage": 1}}}) == [
        ("SST-CFG004", "config:coco_desktop_profiles.analyst.+stage")
    ]
    # Root-only keys may be set only at the root of their block.
    assert _codes({"skills": {"finance": {"+threads": 2}}}) == [("SST-CFG055", "config:skills.finance.+threads")]
    assert _codes({"plugins": {"+threads": 2}}) == [("SST-CFG003", "config:plugins.+threads")]
    assert _codes({"semantic_views": {"+tags": [], "+max_staleness": 60}}) == [
        ("SST-CFG023", "config:semantic_views.+max_staleness"),
    ]


def test_0_3_blocks_are_removed_with_their_reasons_and_deploy_is_deprecated() -> None:
    assert _codes({"generation": {"threads": 1, "publish_via": "ddl"}, "defer": {}}) == [
        ("SST-CFG043", "config:generation.publish_via"),
    ]
    assert _codes({"validation": {"exclude_dirs": []}}) == []
    assert _codes({"defer": {"auto_compile": "yes"}}) == [("SST-CFG004", "config:defer.auto_compile")]
    assert _codes({"deploy": {"agent_spec_stage": {"stage": "S"}}}) == [("SST-CFG200", "config:deploy")]
    assert _codes({"deploy": {"bogus": 1}}) == [("SST-CFG200", "config:deploy"), ("SST-CFG003", "config:deploy.bogus")]
    assert _codes({"apply": {"fail_fast": True}}) == []
    assert _codes({"apply": {"fail_fast": "yes"}}) == [("SST-CFG004", "config:apply.fail_fast")]


def test_types_domains_and_bounds() -> None:
    assert _codes({"project": {"skills_dir": 3}}) == [("SST-CFG004", "config:project.skills_dir")]
    assert _codes({"agents": {"+query_timeout": True}}) == [("SST-CFG004", "config:agents.+query_timeout")]
    assert _codes({"tags": {"pii": "not-a-block"}}) == [("SST-CFG004", "config:tags.pii")]
    assert _codes({"semantic_views": {"core": "flat"}}) == [("SST-CFG004", "config:semantic_views.core")]
    assert _codes({"vars": []}) == [("SST-CFG004", "config:vars")]
    assert _codes({"tools": {"+execute_as": "sudo"}}) == [("SST-CFG008", "config:tools.+execute_as")]
    assert _codes({"tools": {"+execute_as": "restricted caller"}}) == []
    # Matched as a member's execute_as is, ignoring case and runs of spaces.
    assert _codes({"tools": {"+execute_as": "OWNER"}}) == []
    assert _codes({"tools": {"+execute_as": "Restricted   Caller"}}) == []
    # Every other enum still matches exactly.
    assert _codes({"diagnostics": {"severity_overrides": {"SST-VAL003": "WARNING"}}}) == [
        ("SST-CFG008", "config:diagnostics.severity_overrides.SST-VAL003")
    ]
    # The keys every channel once had to fix are gone: their behaviour is the only one there is.
    assert _codes({"coco_desktop_profiles": {"+layout": "by_type", "+flatten": False}}) == [
        ("SST-CFG003", "config:coco_desktop_profiles.+layout"),
        ("SST-CFG003", "config:coco_desktop_profiles.+flatten"),
    ]
    for threads in (0, 17):
        assert _codes({"skills": {"+threads": threads}}) == [("SST-CFG008", "config:skills.+threads")]
    assert "1..16" in validate_config({"skills": {"+threads": 0}})[0].message


def test_no_key_is_required_and_an_extension_entry_takes_only_fqn() -> None:
    assert _codes({"skills": {"+certified": True}, "coco_desktop_profiles": {}}) == []
    assert _codes({"extensions": {"x": {"fqn": "A.B.C", "other": 1}}}) == [("SST-CFG003", "config:extensions.x.other")]


def test_empty_values_mean_unset_except_for_a_plus_key() -> None:
    assert _codes({"tags": {"pii": None}, "apply": {"agent_spec_stage": {"schema": None}}}) == []
    assert _codes({"skills": None}) == []
    assert _codes({"agents": {"+warehouse": None}}) == [("SST-CFG054", "config:agents.+warehouse")]
    assert _codes({"skills": {"finance": {"+bundle_stage": None}}}) == [
        ("SST-CFG054", "config:skills.finance.+bundle_stage")
    ]


def test_every_kind_has_a_representative_key() -> None:
    kinds = {key.kind for key in CONFIG_SCHEMA}
    assert kinds == set(KeyKind)


def test_the_enrichment_block_is_read_and_its_limits_are_checked() -> None:
    accepted = {
        "enrichment": {
            "distinct_limit": 25,
            "sample_values_display_limit": 10,
            "synonym_model": "claude-sonnet-4-6",
            "synonym_max_count": 4,
            "allow_sample_value_collection": False,
        }
    }
    assert _codes(accepted) == []
    assert _codes({"enrichment": {"distinct_limit": 0, "synonym_max_count": 21}}) == [
        ("SST-CFG008", "config:enrichment.distinct_limit"),
        ("SST-CFG008", "config:enrichment.synonym_max_count"),
    ]
    assert _codes({"enrichment": {"allow_sample_value_collection": "no", "infer_is_enum": True}}) == [
        ("SST-CFG004", "config:enrichment.allow_sample_value_collection"),
        ("SST-CFG003", "config:enrichment.infer_is_enum"),
    ]


def test_the_display_limit_cannot_exceed_the_distinct_limit() -> None:
    above = validate_config({"enrichment": {"distinct_limit": 5, "sample_values_display_limit": 6}})
    assert [(item.code, item.subject) for item in above] == [
        ("SST-CFG008", "config:enrichment.sample_values_display_limit")
    ]
    assert "outside 1..5 (enrichment.distinct_limit)" in above[0].message
    # The default distinct limit is 25, so 26 alone is too many; 25 is not.
    assert _codes({"enrichment": {"sample_values_display_limit": 26}}) == [
        ("SST-CFG008", "config:enrichment.sample_values_display_limit")
    ]
    assert _codes({"enrichment": {"sample_values_display_limit": 25}}) == []
    # A limit of the wrong type is reported on its own, not compared.
    assert _codes({"enrichment": {"distinct_limit": "5", "sample_values_display_limit": 6}}) == [
        ("SST-CFG004", "config:enrichment.distinct_limit")
    ]
    assert _codes({"enrichment": {"distinct_limit": 5, "sample_values_display_limit": True}}) == [
        ("SST-CFG004", "config:enrichment.sample_values_display_limit")
    ]


def test_only_legal_severity_overrides_take_effect() -> None:
    from snowflake_semantic_tools.domain.validate.config import severity_overrides

    tree = {
        "diagnostics": {
            "severity_overrides": {
                "SST-VAL003": "error",
                "SST-REF001": "info",
                "SST-NOPE01": "warning",
                "SST-CFG018": "fatal",
            }
        }
    }
    assert severity_overrides(tree) == {"SST-VAL003": Severity.ERROR}
    assert _codes(tree) == [
        ("SST-CFG008", "config:diagnostics.severity_overrides.SST-CFG018"),
        ("SST-CFG033", "config:diagnostics.severity_overrides.SST-REF001"),
        ("SST-CFG003", "config:diagnostics.severity_overrides.SST-NOPE01"),
    ]
    assert severity_overrides({"diagnostics": []}) == {}


def test_key_summaries_describe_what_the_renderer_and_the_publishing_blocks_do() -> None:
    # Tags render inline in the view's CREATE, not in a statement after it.
    tags = CONFIG_KEYS["semantic_views.+tags"].summary
    assert "WITH TAG" in tags and "after the CREATE" not in tags
    # A profile publishes only where its stage resolves; `+threads` is set once, for the run.
    profiles = CONFIG_KEYS["coco_desktop_profiles"].summary
    assert "`+stage` resolves" in profiles
    assert CONFIG_KEYS["skills.+threads"].root_only and not CONFIG_KEYS["skills.+bundle_stage"].root_only


def test_the_execute_as_default_accepts_exactly_what_a_procedure_member_does() -> None:
    assert CONFIG_KEYS["tools.+execute_as"].choices == EXECUTE_AS


def test_manifest_schema_versions_reports_each_entry_the_reader_would_drop() -> None:
    found = validate_config(
        {"dbt": {"manifest_schema_versions": [12, True, "13"]}},
        positions={("dbt", "manifest_schema_versions"): (4, 3)},
    )
    assert [(item.code, item.message) for item in found] == [
        ("SST-CFG004", "config key 'dbt.manifest_schema_versions[1]' expects integer, found bool"),
        ("SST-CFG004", "config key 'dbt.manifest_schema_versions[2]' expects integer, found str"),
    ]
    assert {(item.origin.line, item.origin.col) for item in found if item.origin} == {(4, 3)}
    [empty] = validate_config({"dbt": {"manifest_schema_versions": []}})
    assert (empty.code, empty.subject) == ("SST-CFG008", "config:dbt.manifest_schema_versions")
    assert _codes({"dbt": {"manifest_schema_versions": [11, 12]}}) == []
    assert _codes({"dbt": {"manifest_schema_versions": "12"}}) == [
        ("SST-CFG004", "config:dbt.manifest_schema_versions")
    ]
    assert _codes({"dbt": {"invoke": False}}) == []


def test_manifest_schema_versions_says_it_replaces_the_default_set() -> None:
    assert "replaces the default set" in CONFIG_KEYS["dbt.manifest_schema_versions"].summary


def test_a_refused_location_is_removed_wherever_it_is_written_and_the_parse_left_alone() -> None:
    routed = {"ops": {"+schema": "A B", "+alias": "x"}, 7: {"+schema": " "}}
    tree: dict[str, object] = {"agents": routed, "state": {"+database": 5}}
    pruned, diagnostics = validated_config(tree)
    assert [item.code for item in diagnostics] == ["SST-CFG008", "SST-CFG008", "SST-CFG004"]
    assert pruned == {"agents": {"ops": {"+alias": "x"}, 7: {}}, "state": {}}
    # The tree it was given keeps every value, for the readers that hash it as written.
    assert tree["agents"] is routed and routed["ops"] == {"+schema": "A B", "+alias": "x"}


def test_a_location_is_usable_when_its_readers_can_parse_it() -> None:
    assert not unusable_name("", KeyKind.IDENTIFIER)
    assert not unusable_name("{{ target.database }}", KeyKind.IDENTIFIER)
    assert not unusable_name('"a.b"', KeyKind.IDENTIFIER)
    assert unusable_name("A.B", KeyKind.IDENTIFIER)
    assert unusable_name("{{ target.database }} X", KeyKind.IDENTIFIER)
    assert not unusable_name("{{ target.database }}.{{ target.schema }}.STAGE", KeyKind.OBJECT_NAME)
    assert unusable_name("A.B", KeyKind.OBJECT_NAME)
    assert unusable_name("@stage", KeyKind.OBJECT_NAME)
    assert not unusable_name("{{ target.database }}.SCRATCH", KeyKind.SCHEMA_NAME)
    assert not unusable_name('USER$JDOE."My Schema"', KeyKind.SCHEMA_NAME)
    assert unusable_name("SCRATCH", KeyKind.SCHEMA_NAME)
    assert unusable_name("DB.S.T", KeyKind.SCHEMA_NAME)


def test_the_scratch_schema_takes_a_two_part_name_at_the_root_in_a_route_and_in_an_agents_config() -> None:
    assert CONFIG_KEYS["agents.+scratch_schema"].kind is KeyKind.SCHEMA_NAME
    assert validate_config({"agents": {"+scratch_schema": "DEV_DB.SCRATCH", "ops": {"+scratch_schema": "A.B"}}}) == ()
    [refused] = validate_config({"agents": {"+scratch_schema": "SCRATCH"}})
    assert (refused.code, refused.message) == (
        "SST-CFG008",
        "config key 'agents.+scratch_schema' value 'SCRATCH' is outside two-part names, DATABASE.SCHEMA",
    )
    [mistyped] = validate_config({"agents": {"+scratch_schema": 3}})
    assert mistyped.code == "SST-CFG004"
