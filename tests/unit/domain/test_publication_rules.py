"""The publication guards, rule by rule: versions, schemas, uploads, pointers, statements and channels."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import CompositeObservation, PhysicalResource
from snowflake_semantic_tools.domain.render.eval import dataset_version_comment
from snowflake_semantic_tools.domain.validate.publication import (
    MintedVersion,
    channel_outcome_diagnostics,
    eval_publication_diagnostics,
    put_target_diagnostics,
    registry_pointer_diagnostics,
    schema_diagnostics,
    statement_diagnostic,
    version_collision_diagnostics,
    version_coverage_diagnostics,
    version_diagnostics,
)
from tests.helpers.artifact_builders import rendered
from tests.helpers.sql_values import statement as sql_text

DIGEST = "abcdef0123456789" + "0" * 48
TARGET = QualifiedName.parse("DB.CATALOG.SKILL")


def _version(alias: str, digest: str = DIGEST, target: QualifiedName = TARGET) -> MintedVersion:
    return MintedVersion("skill:s", "s", target, alias, digest)


def test_a_version_is_named_by_the_prefix_and_its_digest() -> None:
    assert version_diagnostics(_version("SST_ABCDEF012345"), "SST_") == ()
    [found] = version_diagnostics(_version("SST_LATEST"), "SST_")
    assert (found.code, found.context["value"]) == ("SST-VAL803", "SST_LATEST")


def test_one_name_for_two_bundles_at_one_extension_collides() -> None:
    same = (_version("V1"), _version("v1"))
    assert version_collision_diagnostics(same) == ()
    [found] = version_collision_diagnostics((_version("V1"), _version("V1", "f" * 64)))
    assert found.code == "SST-VAL825"
    elsewhere = _version("V1", "f" * 64, QualifiedName.parse("DB.OTHER.SKILL"))
    assert version_collision_diagnostics((_version("V1"), elsewhere)) == ()


def test_a_version_outside_the_catalog_schema_is_reported_by_its_schema() -> None:
    assert schema_diagnostics(_version("V"), TARGET.folded[:2]) == ()
    [found] = schema_diagnostics(_version("V"), ("db", "elsewhere"))
    assert (found.code, found.context["value"]) == ("SST-VAL806", "DB.CATALOG")


def test_each_upload_must_target_a_directory() -> None:
    found = put_target_diagnostics("profile:p", "p", ("@S/a/", "@S/b"))
    assert [(item.code, item.context["path"]) for item in found] == [("SST-VAL820", "@S/b")]


def test_each_uploaded_tree_needs_a_pointer_and_a_hashed_prefix() -> None:
    pointers = ("@S/one/skills/a", "@S/two")
    found = registry_pointer_diagnostics("profile:p", "p", ("@S/one/", "@S/three/"), pointers)
    assert [item.context["path"] for item in found] == ["@S/three/"]
    covered = version_coverage_diagnostics("profile:p", "p", ("aaa", "bbb"), "aaa|ccc")
    assert [(item.code, item.context["value"]) for item in covered] == [("SST-VAL823", "bbb")]


@pytest.mark.parametrize(
    ("statement", "pending", "code"),
    [
        ("GRANT USAGE ON X TO ROLE R", False, "SST-VAL827"),
        (sql_text("GRANT USAGE ON X TO ROLE R"), False, None),
        (sql_text("GRANT USAGE ON X TO DATABASE ROLE R"), True, "SST-VAL826"),
        (sql_text("GRANT USAGE ON X TO R"), False, "SST-VAL826"),
        (sql_text("ALTER X SET COMMENT"), True, None),
    ],
)
def test_a_publisher_sends_only_sql_and_typed_grants_after_certification(
    statement: object, pending: bool, code: str | None
) -> None:
    found = statement_diagnostic("skill:s", statement, certification_pending=pending)
    assert (found.code if found else None) == code


def test_each_channel_change_reports_exactly_one_outcome() -> None:
    changes = (("skill:a", "skill"), ("profile:b", "profile"), ("plugin:c", "plugin"), ("semantic_view:v", "x"))
    found = channel_outcome_diagnostics(changes, ("skill:a", "plugin:c", "plugin:c"))
    assert [(item.subject, item.context["detail"]) for item in found] == [
        ("profile:b", "the stage channel reported no outcomes"),
        ("plugin:c", "the catalog channel reported 2 outcomes"),
    ]


def test_an_eval_minting_a_version_has_its_metadata_and_comment_checked() -> None:
    artifact = replace(
        rendered("E"),
        ddl="dataset:\n  name: D\n",
        component_fingerprints=(("dataset_version", "SST_0123456789AB"),),
        version_metadata='{"who":"x"}',
    )
    dataset = PhysicalResource("dataset", QualifiedName.parse("DB.S.D"), True)
    found = eval_publication_diagnostics(artifact, CompositeObservation(artifact.key, resources=(dataset,)))
    assert [item.code for item in found] == ["SST-VAL716", "SST-VAL715"]
    assert found[1].context["detail"] == "a field SST never writes ('who')"
    assert "@" not in dataset_version_comment("SST_0123456789AB")
