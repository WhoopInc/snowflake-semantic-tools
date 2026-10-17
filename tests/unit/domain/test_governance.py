"""Which Snowflake tags make a column sensitive: the `governance:` block, the tag read, and its answer."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.enrich import WarehouseColumn, column_tags_sql, governed_columns, parse_column_tags
from snowflake_semantic_tools.domain.model.config_schema import GovernanceConfig, governance_config, tag_pattern
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.governance import (
    TagPattern,
    matching_tags,
    records_tag,
    sensitivity_reason,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.validate.config import validate_config
from snowflake_semantic_tools.domain.validate.semantic.dbt import dbt_column_diagnostics

# The headings TAG_REFERENCES_ALL_COLUMNS answers with, as measured on a live account.
HEADINGS = (
    "TAG_DATABASE",
    "TAG_SCHEMA",
    "TAG_NAME",
    "TAG_VALUE",
    "LEVEL",
    "OBJECT_DATABASE",
    "OBJECT_SCHEMA",
    "OBJECT_NAME",
    "DOMAIN",
    "COLUMN_NAME",
    "APPLY_METHOD",
)


def _row(tag: str, column: str, level: str = "COLUMN", method: str = "MANUAL") -> tuple[object, ...]:
    database, schema, name = tag.split(".")
    return (database, schema, name, "x", level, "DB", "S", "T", "COLUMN", column, method)


def _tag(text: str) -> QualifiedName:
    return QualifiedName.parse(text)


def test_an_entry_names_one_tag_or_a_whole_schema_and_matches_without_case_when_unquoted() -> None:
    one = TagPattern.parse(" sec.gov.email ")
    schema = TagPattern.parse('SEC."Gov".*')
    assert (one.text, schema.text) == ("SEC.GOV.EMAIL", 'SEC."Gov".*')
    assert one.matches(_tag("SEC.GOV.EMAIL")) and not one.matches(_tag("SEC.GOV.PHONE"))
    assert not one.matches(_tag("OTHER.GOV.EMAIL"))
    assert schema.matches(_tag('SEC."Gov".ANY')) and not schema.matches(_tag("SEC.GOV.ANY"))
    for bad in ("SEC.GOV", "SEC.*", "SEC.GOV.TAG.X", '"SEC.*', "SEC.GOV.*.X", ""):
        with pytest.raises(ValueError):
            TagPattern.parse(bad)


def test_matching_tags_are_named_as_sql_names_them_sorted_and_once() -> None:
    patterns = (TagPattern.parse("SEC.GOV.*"), TagPattern.parse("SEC.GOV.EMAIL"))
    tags = (_tag("SEC.GOV.PHONE"), _tag("SEC.GOV.EMAIL"), _tag("OTHER.S.T"), _tag("sec.gov.email"))
    assert matching_tags(tags, patterns) == ("SEC.GOV.EMAIL", "SEC.GOV.PHONE")
    assert matching_tags(tags, ()) == ()


def test_a_column_records_a_tag_by_pii_tags_or_by_listing_it() -> None:
    listed = DbtColumn("email", None, None, None, governance_tags=("sec.gov.email", "not a name"))
    assert records_tag(listed, "SEC.GOV.EMAIL") and records_tag(listed, "NOT A NAME")
    assert not records_tag(listed, "SEC.GOV.PHONE")
    assert records_tag(DbtColumn("email", None, None, None, pii_tagged=True), "SEC.GOV.PHONE")
    assert not records_tag(None, "SEC.GOV.EMAIL")


def test_the_reason_a_column_is_sensitive_prefers_pii_tags_then_recorded_then_found_tags() -> None:
    plain = DbtColumn("email", None, None, None)
    assert sensitivity_reason(DbtColumn("e", None, None, None, pii_tagged=True, governance_tags=("A.B.C",))) == (
        "pii_tags"
    )
    assert sensitivity_reason(DbtColumn("e", None, None, None, governance_tags=("A.B.C",)), ("X.Y.Z",)) == "A.B.C"
    assert sensitivity_reason(plain, ("X.Y.Z",)) == "X.Y.Z"
    assert sensitivity_reason(plain) == "pii_tags"
    assert not plain.sensitive and DbtColumn("e", None, None, None, governance_tags=("A.B.C",)).sensitive


def test_the_governance_block_reads_only_entries_that_name_tags_and_reads_nothing_unless_on() -> None:
    assert governance_config({}) == GovernanceConfig() and not GovernanceConfig().reads_tags
    config = governance_config({"governance": {"sensitive_tags": ["sec.gov.*", 7, "bad"], "read_column_tags": True}})
    assert config.sensitive_tags == (TagPattern.parse("SEC.GOV.*"),) and config.reads_tags
    assert not governance_config({"governance": {"sensitive_tags": ["A.B.C"], "read_column_tags": "yes"}}).reads_tags
    assert not governance_config({"governance": {"sensitive_tags": "A.B.C", "read_column_tags": True}}).reads_tags
    assert tag_pattern(None) is None and tag_pattern("A.B.C") == TagPattern.parse("A.B.C")


def test_validation_refuses_each_entry_that_names_no_tag_and_checks_the_block_types() -> None:
    found = validate_config({"governance": {"sensitive_tags": ["A.B.C", "A.B", True], "read_column_tags": False}})
    assert [(item.code, item.context["key"]) for item in found] == [
        ("SST-CFG057", "governance.sensitive_tags[1]"),
        ("SST-CFG057", "governance.sensitive_tags[2]"),
    ]
    mistyped = validate_config({"governance": {"sensitive_tags": "A.B.C", "read_column_tags": "true"}})
    assert [item.code for item in mistyped] == ["SST-CFG004", "SST-CFG004"]
    assert list(validate_config({"governance": {"sensitive_tags": [], "read_column_tags": True}})) == []


def test_the_tag_read_names_its_relation_by_a_constant_in_its_databases_information_schema() -> None:
    statement = str(column_tags_sql(QualifiedName.parse('db.sch."Orders"')))
    assert statement == (
        "SELECT * FROM TABLE(DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('DB.SCH.\"Orders\"', 'table'))"
    )
    assert "''" in str(column_tags_sql(QualifiedName.parse('db.sch."it\'s"')))


def test_every_level_and_apply_method_counts_and_each_tag_is_kept_once_per_column() -> None:
    result = QueryResult(
        HEADINGS,
        (
            _row("SEC.GOV.EMAIL", "A"),
            _row("SEC.GOV.INTERNAL", "A", "SCHEMA", "INHERITED"),
            _row("SEC.GOV.INTERNAL", "B", "SCHEMA", "INHERITED"),
            _row("SEC.GOV.EMAIL", "a", "TABLE", "PROPAGATED"),
            _row("SEC.GOV.SSN", "Mixed", "COLUMN", "CLASSIFIED"),
            (None, "GOV", "X", "x", "COLUMN", "DB", "S", "T", "COLUMN", "C", "MANUAL"),
            ("SEC", "GOV"),
        ),
    )
    assert parse_column_tags(result) == {
        "a": (_tag("SEC.GOV.EMAIL"), _tag("SEC.GOV.INTERNAL")),
        "b": (_tag("SEC.GOV.INTERNAL"),),
        "mixed": (_tag("SEC.GOV.SSN"),),
    }
    lower = QueryResult(tuple(heading.lower() for heading in HEADINGS), (_row("SEC.gov.T", "A"),))
    assert parse_column_tags(lower) == {"a": (QualifiedName.parse('SEC."gov".T'),)}
    assert parse_column_tags(QueryResult(("TAG_NAME", "COLUMN_NAME"), ())) is None


def test_governed_columns_carry_the_sensitive_tags_found_on_them() -> None:
    columns = (WarehouseColumn("EMAIL", "TEXT"), WarehouseColumn("NOTE", "TEXT"))
    tags = {"email": (_tag("SEC.GOV.EMAIL"), _tag("OTHER.S.T")), "note": (_tag("OTHER.S.T"),)}
    assert governed_columns(columns, tags, (TagPattern.parse("SEC.GOV.*"),)) == (
        WarehouseColumn("EMAIL", "TEXT", ("SEC.GOV.EMAIL",)),
        WarehouseColumn("NOTE", "TEXT"),
    )


def test_validate_reports_sample_values_on_a_column_its_yaml_marks_sensitive() -> None:
    columns = (
        DbtColumn("email", "e", "TEXT", "dimension", sample_values=("a@b.c",), pii_tagged=True),
        DbtColumn("phone", "p", "TEXT", "dimension", sample_values=("1", "2"), governance_tags=("SEC.GOV.PHONE",)),
        DbtColumn("ssn", "s", "TEXT", "dimension", governance_tags=("SEC.GOV.SSN",)),
        DbtColumn("status", "s", "TEXT", "dimension", sample_values=("open",)),
    )
    model = DbtModel("model.p.orders", "orders", "DB.S.ORDERS", ("email",), (), columns)
    found = [item for item in dbt_column_diagnostics({"orders": model}) if item.code == "SST-VAL328"]
    assert [item.message for item in found] == [
        "model 'orders': column 'email' carries pii_tags and 1 sample_values",
        "model 'orders': column 'phone' carries SEC.GOV.PHONE and 2 sample_values",
    ]
    assert found[1].subject == "dbt_column:orders.phone"
    assert [item for item in dbt_column_diagnostics({"orders": model}, frozenset()) if item.code == "SST-VAL328"] == []
