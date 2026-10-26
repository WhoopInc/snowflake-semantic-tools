"""Small domain behaviors the broader suites leave unexercised."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.coco_desktop_profile.model import CocoDesktopProfileRelease
from snowflake_semantic_tools.domain.model.pii_shape import pii_shape
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import live_view_from_yaml
from snowflake_semantic_tools.domain.validate.skill_files import check_skill_files
from tests.helpers.skill_inputs import skill


def test_a_value_on_an_ip_column_that_is_not_an_address_is_no_pii_shape() -> None:
    assert pii_shape("999.1.2.3", "client_ip") is None
    assert pii_shape("10.0.0.1", "client_ip") == "an IP address"


def test_a_credential_key_inside_a_shell_expansion_is_not_a_literal() -> None:
    authored = skill(files={"scripts/run.sh": "echo ${API_KEY:-abcdefgh12345\n"})
    found = [item.context.get("detail") for item in check_skill_files(authored) if item.code == "SST-VAL815"]
    assert "a credential literal" not in found


def _members(document: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    return {member.key: dict(member.properties) for member in live_view_from_yaml(document).members}


def test_a_yaml_fact_carries_no_enum_flag_and_a_verified_query_reads_every_field() -> None:
    members = _members(
        {
            "tables": [{"name": "orders", "facts": [{"name": "amount", "expr": "amount"}]}],
            "verified_queries": [
                {
                    "name": "total",
                    "question": "What is the total?",
                    "sql": "SELECT 1",
                    "verified_at": 1700000000,
                    "verified_by": "analyst",
                    "use_as_onboarding_question": True,
                },
                {"name": "unverified"},
            ],
        }
    )
    assert "is_enum" not in members[("fact", "orders.amount")]
    assert members[("verified_query", "total")] == {
        "question": "What is the total?",
        "sql": "SELECT 1",
        "verified_at": "1700000000",
        "verified_by": "analyst",
        "onboarding_question": "true",
    }
    assert members[("verified_query", "unverified")]["verified_at"] == ""


def test_a_metric_names_over_only_as_a_window_outside_quotes_and_words() -> None:
    members = _members(
        {
            "tables": [],
            "metrics": [
                {"name": "quoted", "expr": "COUNT_IF(x = ' OVER (')"},
                {"name": "worded", "expr": "SUM(x) + COVER_COUNT + OVERALL"},
                {"name": "windowed", "expr": "SUM(x) OVER (PARTITION BY y)"},
            ],
        }
    )
    assert "expression" in members[("metric", "quoted")]
    assert "expression" in members[("metric", "worded")]
    assert "window_expression" in members[("metric", "windowed")]


def test_a_coco_desktop_release_without_a_config_name_takes_its_profile_name() -> None:
    release = CocoDesktopProfileRelease(name="analyst", content="c", trees=(), row={}, source_files=())
    assert release.config_name == "analyst"
    assert CocoDesktopProfileRelease("analyst", "c", (), {}, (), config_name="analyst_dev").config_name == "analyst_dev"
