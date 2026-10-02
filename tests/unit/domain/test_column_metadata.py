"""The column-metadata rules validation and `sst enrich` share."""

from __future__ import annotations

from snowflake_semantic_tools.domain.validate.column_metadata import (
    base_type,
    is_numeric,
    is_sentinel,
    is_temporal,
    printable,
    synonym_problem,
)


def test_a_base_type_drops_parameters_and_case() -> None:
    assert base_type(" number(38, 0) ") == "NUMBER"
    assert base_type("varchar") == "VARCHAR"
    assert base_type(None) == ""


def test_numeric_and_temporal_types_are_recognised_by_base_type() -> None:
    assert [is_numeric(value) for value in ("NUMBER(10,2)", "float", "TEXT", None)] == [True, True, False, False]
    assert [is_temporal(value) for value in ("timestamp_ntz(9)", "DATE", "BOOLEAN")] == [True, True, False]


def test_sentinels_are_missing_values_in_any_case() -> None:
    assert [is_sentinel(value) for value in ("nan", "NaN", "None", "NULL", "<NA>", "NaT", "n/a")] == [
        True,
        True,
        True,
        True,
        True,
        False,
        False,
    ]


def test_a_synonym_with_a_quote_is_unusable() -> None:
    single = "customer" + chr(39) + "s name"
    assert [synonym_problem(value) for value in ("order count", single, 'say "hi"')] == [None, "quotes", "quotes"]


def test_a_synonym_with_a_control_or_invisible_character_is_unusable() -> None:
    values = ("line\nbreak", "tab\there", "bell\x07", "rtl\u202eoverride", "zero\u200bwidth", "para\u2029sep")
    assert {synonym_problem(value) for value in values} == {"control characters"}
    assert synonym_problem("caf\u00e9 total") is None


def test_printable_escapes_control_characters_and_cuts_long_text() -> None:
    assert printable("a\x1b[31mred\nb") == "a\\u001b[31mred\\u000ab"
    assert printable("plain") == "plain"
    assert printable("x" * 81) == "x" * 77 + "..."
    assert printable("x" * 80) == "x" * 80
