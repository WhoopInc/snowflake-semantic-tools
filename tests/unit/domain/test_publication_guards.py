"""The publication guards, each on the smallest input it reports and the nearest one it does not.

The diagnostic-code tests pin each code's message through the pipeline; these pin every
guard's own shapes: each sensitive pattern, malformed METADATA, and an eval that mints no
dataset version among them.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import CompositeObservation, PhysicalResource
from snowflake_semantic_tools.domain.render.eval import dataset_version_comment
from snowflake_semantic_tools.domain.sql import ident, qname, sql
from snowflake_semantic_tools.domain.validate.publication import (
    MintedVersion,
    channel_outcome_diagnostics,
    dataset_metadata_diagnostics,
    eval_publication_diagnostics,
    put_target_diagnostics,
    registry_pointer_diagnostics,
    run_config_diagnostics,
    schema_diagnostics,
    statement_diagnostic,
    version_collision_diagnostics,
    version_coverage_diagnostics,
    version_diagnostics,
)
from tests.helpers.artifact_builders import rendered

CLEAN = '{"agent":"DB.S.AGENT","dataset_fingerprint":"' + "0" * 64 + '","source_table":"DB.S.SRC"}'
TARGET = QualifiedName.parse("DB.S.MONTH_CLOSE")
DIGEST = "0123456789ab" + "0" * 52


def _version(alias: str = "GIT_0123456789AB", digest: str = DIGEST, target: QualifiedName = TARGET) -> MintedVersion:
    return MintedVersion("skill:month-close", "month-close", target, alias, digest)


@pytest.mark.parametrize(
    ("metadata", "comment", "expected"),
    [
        (CLEAN, "SST eval questions 123-45-6789", [("COMMENT", "a US social security number")]),
        (CLEAN, "card 4111 1111 1111 1111", [("COMMENT", "a payment card number")]),
        ("not json", "SST", [("METADATA", "text that is not a JSON object")]),
        ('["DB.S.AGENT"]', "SST", [("METADATA", "text that is not a JSON object")]),
        ('{"agent":"a@example.com"}', "SST", [("METADATA", "an email address")]),
        ('{"owner":"DB.S.AGENT"}', "SST", [("METADATA", "a field SST never writes ('owner')")]),
        (CLEAN, dataset_version_comment("SST_0123456789AB"), []),
    ],
)
def test_dataset_metadata_reports_each_field_by_what_it_matches(
    metadata: str, comment: str, expected: list[tuple[str, str]]
) -> None:
    found = dataset_metadata_diagnostics("eval:e", metadata=metadata, comment=comment)
    assert [(item.context["field"], item.context["detail"]) for item in found] == expected


def test_a_config_without_a_dataset_block_is_quiet_whether_or_not_the_dataset_exists() -> None:
    config = "evaluation:\n  source_metadata:\n    dataset_name: D\nmetrics: []\n"
    assert run_config_diagnostics("eval:e", config, dataset_exists=True) == ()


def test_a_dataset_block_is_reported_only_when_the_dataset_exists() -> None:
    config = "dataset:\n  name: D\nevaluation: {}\n"
    assert [item.code for item in run_config_diagnostics("eval:e", config, dataset_exists=True)] == ["SST-VAL716"]
    assert run_config_diagnostics("eval:e", config, dataset_exists=False) == ()


def test_an_artifact_that_mints_no_version_has_only_its_config_checked() -> None:
    artifact = rendered()
    assert eval_publication_diagnostics(artifact, CompositeObservation(artifact.key)) == ()


def test_an_eval_has_its_minted_version_checked_against_an_existing_dataset() -> None:
    components = (("dataset_version", "SST_0123456789AB"),)
    artifact = replace(rendered(), component_fingerprints=components, version_metadata='{"owner":"x"}')
    resources = (
        PhysicalResource("TABLE", TARGET, True),
        PhysicalResource("dataset", QualifiedName.parse("DB.S.QUESTIONS"), False),
    )
    found = eval_publication_diagnostics(artifact, CompositeObservation(artifact.key, resources))
    assert [(item.code, item.context["field"]) for item in found] == [("SST-VAL715", "METADATA")]


def test_a_version_is_named_by_the_prefix_and_the_digest_leading_hex() -> None:
    assert version_diagnostics(_version(), "GIT_") == ()
    [found] = version_diagnostics(_version("GIT_FFFFFFFFFFFF"), "GIT_")
    assert (found.code, found.subject) == ("SST-VAL803", "skill:month-close")


def test_a_version_name_collides_only_for_other_content_at_the_same_extension() -> None:
    elsewhere = QualifiedName.parse("DB.S.OTHER")
    assert version_collision_diagnostics((_version(), _version(), _version(digest="f" * 64, target=elsewhere))) == ()
    [found] = version_collision_diagnostics((_version(), _version(digest="1" * 64)))
    assert found.code == "SST-VAL825"


def test_a_version_outside_the_catalog_schema_is_reported() -> None:
    assert schema_diagnostics(_version(), TARGET.folded[:2]) == ()
    [found] = schema_diagnostics(_version(), ("OTHER", "S"))
    assert (found.code, found.context["value"]) == ("SST-VAL806", "DB.S")


def test_each_upload_location_must_be_a_directory() -> None:
    found = put_target_diagnostics("skill:month-close", "month-close", ("@DB.S.STAGE/a/", "@DB.S.STAGE/b"))
    assert [(item.code, item.context["path"]) for item in found] == [("SST-VAL820", "@DB.S.STAGE/b")]


def test_each_uploaded_tree_needs_a_pointer_in_the_registry_row() -> None:
    uploads = ("@DB.S.STAGE/a", "@DB.S.STAGE/b")
    found = registry_pointer_diagnostics("profile:p", "p", uploads, ("@DB.S.STAGE/a/SKILL.md",))
    assert [(item.code, item.context["path"]) for item in found] == [("SST-VAL822", "@DB.S.STAGE/b")]


def test_each_shipped_tree_prefix_must_be_hashed_into_the_version() -> None:
    found = version_coverage_diagnostics("profile:p", "p", ("a1", "b2"), "a1:digest")
    assert [(item.code, item.context["value"]) for item in found] == [("SST-VAL823", "b2")]


def test_a_publisher_sends_typed_sql_and_typed_grants_only() -> None:
    raw = statement_diagnostic("skill:month-close", "DROP TABLE x", certification_pending=False)
    assert raw is not None and (raw.code, raw.context["value"]) == ("SST-VAL827", "DROP TABLE x")
    grant = sql(
        "GRANT USAGE ON CORTEX EXTENSION {target} TO ROLE {role}",
        target=qname(TARGET),
        role=ident(Identifier.parse("ANALYST")),
    )
    assert statement_diagnostic("skill:month-close", grant, certification_pending=False) is None
    early = statement_diagnostic("skill:month-close", grant, certification_pending=True)
    assert early is not None and early.code == "SST-VAL826"
    assert statement_diagnostic("skill:month-close", sql("SELECT 1"), certification_pending=True) is None


def test_every_channel_change_reports_exactly_one_outcome() -> None:
    changes = (
        ("skill:a", "skill"),
        ("plugin:b", "plugin"),
        ("profile:c", "profile"),
        ("semantic_view:v", "semantic_view"),
    )
    found = channel_outcome_diagnostics(changes, ("skill:a", "plugin:b", "plugin:b"))
    assert [(item.subject, item.context["detail"]) for item in found] == [
        ("plugin:b", "the catalog channel reported 2 outcomes"),
        ("profile:c", "the stage channel reported no outcomes"),
    ]
