"""Snowflake identifier and target identity contracts."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.identifier import (
    Identifier,
    QualifiedName,
    SchemaScope,
    TargetIdentity,
    production_like,
)


def test_identifier_folding_and_quoting() -> None:
    plain = Identifier.parse("mixed_$1")
    quoted = Identifier.parse('"Mixed ""Name"""')
    assert (plain.value, plain.folded, plain.sql, plain.quoted) == (
        "MIXED_$1",
        "MIXED_$1",
        "MIXED_$1",
        False,
    )
    assert (quoted.value, quoted.folded, quoted.sql, quoted.quoted) == (
        'Mixed "Name"',
        'Mixed "Name"',
        '"Mixed ""Name"""',
        True,
    )


@pytest.mark.parametrize("value", ['"broken', 'broken"', "has space", "1starts_wrong", ""])
def test_identifier_rejects_malformed_values(value: str) -> None:
    with pytest.raises(ValueError):
        Identifier.parse(value)


def test_qualified_name_retains_quoted_dots_and_builds_scope() -> None:
    name = QualifiedName.parse('DB."Mixed.Schema"."Object.With.Dot"')
    assert name.sql == 'DB."Mixed.Schema"."Object.With.Dot"'
    assert name.folded == ("DB", "Mixed.Schema", "Object.With.Dot")
    assert name.artifact_name == "object.with.dot"
    assert name.artifact_component == '"Object.With.Dot"'
    assert SchemaScope.from_qualified_name(name).sql == 'DB."Mixed.Schema"'
    assert QualifiedName.from_parts("db", "sch", "thing").sql == "DB.SCH.THING"
    assert QualifiedName.from_parts("db", "sch", "thing").artifact_component == "thing"
    escaped = QualifiedName.parse('DB."S"."A""B"')
    assert escaped.name.value == 'A"B'


@pytest.mark.parametrize("value", ["DB.SCHEMA", "DB..NAME", 'DB."SCHEMA.NAME'])
def test_qualified_name_rejects_wrong_shape(value: str) -> None:
    with pytest.raises(ValueError):
        QualifiedName.parse(value)


def test_target_identity_round_trips_with_optional_fields() -> None:
    target = TargetIdentity(
        "verify",
        "EXAMPLE-ACCOUNT",
        Identifier.parse("scratch"),
        Identifier.parse("sst_1_reference_impl"),
        "SST_ROLE",
        "SST_WH",
    )
    assert target.scope.sql == "SCRATCH.SST_1_REFERENCE_IMPL"
    assert target.key == (
        "verify",
        "EXAMPLE-ACCOUNT",
        "SCRATCH",
        "SST_1_REFERENCE_IMPL",
        "SST_ROLE",
        "SST_WH",
    )
    assert TargetIdentity.from_dict(target.as_dict()) == target
    minimal = TargetIdentity.from_dict({"name": "dev", "database": "SCRATCH", "schema": "DEV", "account_locator": 1})
    assert minimal.account_locator == ""
    assert minimal.role is None and minimal.warehouse is None


def test_target_identity_key_changes_with_role_and_warehouse() -> None:
    base = TargetIdentity(
        "verify",
        "account",
        Identifier.parse("db"),
        Identifier.parse("schema"),
        "ROLE_A",
        "WH_A",
    )

    assert (
        base.key
        != TargetIdentity(
            "verify",
            "account",
            Identifier.parse("db"),
            Identifier.parse("schema"),
            "ROLE_B",
            "WH_A",
        ).key
    )
    assert (
        base.key
        != TargetIdentity(
            "verify",
            "account",
            Identifier.parse("db"),
            Identifier.parse("schema"),
            "ROLE_A",
            "WH_B",
        ).key
    )


@pytest.mark.parametrize("value", [None, [], {}, {"name": "x", "database": "db"}])
def test_target_identity_rejects_incomplete_values(value: object) -> None:
    with pytest.raises(ValueError):
        TargetIdentity.from_dict(value)


@pytest.mark.parametrize(
    ("name", "production"),
    [
        ("prod", True),
        ("PROD_us", True),
        ("eu-production", True),
        ("prd.east", True),
        ("product", False),
        ("dev", False),
    ],
)
def test_a_target_is_production_like_only_by_a_whole_word_of_its_name(name: str, production: bool) -> None:
    assert production_like(name) is production
