"""SQL constants Snowflake reads back as exactly the values they were built from."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from snowflake_semantic_tools.domain.sql import (
    DefaultMismatch,
    boolean,
    dollar_quoted,
    literal,
    local_file,
    null,
    number,
    typed_default,
)
from snowflake_semantic_tools.domain.sql.datatype import text_length, type_family
from tests.helpers.sql_reference import snowflake_reads

TRICKY = [
    "plain",
    "the menu item's name",
    "C:\\temp\\new",
    "ends with a backslash\\",
    "a quote after a backslash \\'",
    "line one\nline two",
    "",
    json.dumps({"ground_truth_output": 'multi\nline "quoted" \\ text'}, separators=(",", ":")),
]


@pytest.mark.parametrize("text", TRICKY)
def test_snowflake_reads_back_exactly_the_text(text: str) -> None:
    assert snowflake_reads(str(literal(text))) == text


def test_json_in_a_literal_stays_valid_json() -> None:
    payload = json.dumps({"ground_truth_output": "The reply must state\na count."}, separators=(",", ":"))
    quoted = str(literal(payload))
    assert "\\\\n" in quoted  # JSON's \n reaches PARSE_JSON as the two characters \ and n
    assert json.loads(snowflake_reads(quoted)) == {"ground_truth_output": "The reply must state\na count."}


def test_quote_only_escaping_would_corrupt_the_same_inputs() -> None:
    """The defect `literal` prevents: escaping only quotes turns `\\t` into a tab."""
    naive = "'" + "C:\\temp".replace("'", "''") + "'"
    assert snowflake_reads(naive) == "C:\temp"
    assert snowflake_reads(str(literal("C:\\temp"))) == "C:\\temp"


def test_a_literal_refuses_what_no_snowflake_string_holds() -> None:
    with pytest.raises(ValueError, match="NUL"):
        literal("a\x00b")
    with pytest.raises(ValueError, match="lone surrogate"):
        literal("a\ud800")
    with pytest.raises(TypeError, match="takes str"):
        literal(1)  # type: ignore[arg-type]


def test_numbers_render_only_finite_numeric_values() -> None:
    assert [str(number(value)) for value in (0, -12, 2.5, Decimal("1.50"))] == ["0", "-12", "2.5", "1.50"]
    for value in (True, "1", None):
        with pytest.raises(TypeError, match="number"):
            number(value)  # type: ignore[arg-type]
    for bad in (float("nan"), float("inf"), Decimal("NaN"), Decimal("-Infinity")):
        with pytest.raises(ValueError, match="cannot render"):
            number(bad)


def test_booleans_and_null() -> None:
    assert (str(boolean(True)), str(boolean(False)), str(null())) == ("TRUE", "FALSE", "NULL")
    with pytest.raises(TypeError, match="takes bool"):
        boolean(1)  # type: ignore[arg-type]


def test_a_dollar_quoted_body_cannot_end_its_quoting() -> None:
    assert str(dollar_quoted("\nRETURN 1;\n")) == "$$\nRETURN 1;\n$$"
    with pytest.raises(ValueError, match=r"'\$\$' \(at offset 3\)"):
        dollar_quoted("x; $$ DROP TABLE t; $$")
    with pytest.raises(ValueError, match="end in"):
        dollar_quoted("cost $")
    with pytest.raises(ValueError, match="NUL"):
        dollar_quoted("a\x00")


def test_a_file_uri_keeps_the_path_as_written_unless_it_could_end_the_quote() -> None:
    assert str(local_file("/tmp/sst-upload-1/spec.yaml")) == "'file:///tmp/sst-upload-1/spec.yaml'"
    assert str(local_file("C:\\Temp\\sst")) == "'file://C:\\Temp\\sst'"
    for bad in ("", "/tmp/it's", "C:\\Temp\\", "/tmp/a\nb"):
        with pytest.raises(ValueError, match="file URI"):
            local_file(bad)


@pytest.mark.parametrize(
    ("data_type", "family"),
    [
        ("number(38, 2)", "fixed"),
        ("BYTEINT", "fixed"),
        ("float8", "float"),
        ("Double Precision", "float"),
        ("char varying(4)", "text"),
        ("STRING", "text"),
        ("boolean", "boolean"),
        ("DATE", "date"),
        ("time(3)", "time"),
        ("TIMESTAMP_TZ(9)", "timestamp"),
        ("DATETIME", "timestamp"),
        ("VARIANT", "other"),
        ("VECTOR(INT, 3)", "other"),
        ("BINARY(4)", "other"),
    ],
)
def test_a_type_s_family_groups_its_synonyms(data_type: str, family: str) -> None:
    assert type_family(data_type) == family


def test_a_family_is_only_given_for_a_data_type() -> None:
    with pytest.raises(ValueError, match="is not a Snowflake data type"):
        type_family("NUMBER(")


@pytest.mark.parametrize(
    ("data_type", "value", "rendered"),
    [
        ("NUMBER", 1, "1"),
        ("INT", 3, "3"),
        ("FLOAT", 1.5, "1.5"),
        ("DECIMAL(10, 2)", Decimal("2.50"), "2.50"),
        ("BOOLEAN", False, "FALSE"),
        ("VARCHAR(10)", "it's", "'it''s'"),
        ("DATE", "2026-01-01", "'2026-01-01'::DATE"),
        ("TIME", " 13:30:00.5 ", "'13:30:00.5'::TIME"),
        ("TIMESTAMP_NTZ", "2026-01-01 10:00:00", "'2026-01-01 10:00:00'::TIMESTAMP_NTZ"),
        ("TIMESTAMP_TZ(3)", "2026-01-01T10:00:00+02:00", "'2026-01-01T10:00:00+02:00'::TIMESTAMP_TZ(3)"),
        ("TIMESTAMP_LTZ", "2026-01-01", "'2026-01-01'::TIMESTAMP_LTZ"),
    ],
)
def test_a_default_renders_as_a_constant_of_its_type(data_type: str, value: object, rendered: str) -> None:
    assert str(typed_default(data_type, value)) == rendered


@pytest.mark.parametrize(
    ("data_type", "value", "expected"),
    [
        ("NUMBER", "abc", "a finite number"),
        ("INT", True, "a finite number"),
        ("FLOAT", float("nan"), "a finite number"),
        ("BOOLEAN", 1, "true or false"),
        ("TEXT", 3, "a string"),
        ("TEXT", "a\x00b", "a string"),
        ("DATE", "2026-02-30T00:00", "an ISO date such as 2026-01-01"),
        ("DATE", None, "an ISO date such as 2026-01-01"),
        ("TIME", "25:00", "an ISO time such as 13:30:00"),
        ("TIMESTAMP", "now", "an ISO date and time such as 2026-01-01 13:30:00"),
        ("VARIANT", "{}", "no default SST can write; use a numeric, text, BOOLEAN, DATE, TIME or TIMESTAMP type"),
    ],
)
def test_a_default_its_type_cannot_hold_is_refused(data_type: str, value: object, expected: str) -> None:
    with pytest.raises(DefaultMismatch) as raised:
        typed_default(data_type, value)
    assert raised.value.expected == expected
    assert str(raised.value) == f"a {data_type} default must be {expected}"


def test_text_length_reads_a_bounded_text_type_and_nothing_else() -> None:
    assert text_length("VARCHAR(3)") == 3
    assert text_length("char( 15 )") == 15
    assert text_length("VARCHAR(16777216)") is None
    assert text_length("TEXT") is None
    assert text_length("NUMBER(38,0)") is None
    assert text_length("BINARY(8)") is None
