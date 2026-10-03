"""SQL constants Snowflake reads back as exactly the values they were built from."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from snowflake_semantic_tools.domain.sql import boolean, dollar_quoted, literal, local_file, null, number
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
