"""String literals Snowflake reads back as the exact text they were built from."""

from __future__ import annotations

import json

import pytest

from snowflake_semantic_tools.domain.model.sql import string_literal

# Snowflake's single-quoted literal escapes, as its documentation lists them.
_ESCAPES = {"'": "'", '"': '"', "\\": "\\", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "0": "\0"}


def snowflake_reads(literal: str) -> str:
    """What Snowflake stores for a single-quoted literal: `''` and backslash escapes resolved."""
    assert literal[0] == literal[-1] == "'", literal
    body, out, index = literal[1:-1], [], 0
    while index < len(body):
        char = body[index]
        if char == "\\":
            index += 1
            out.append(_ESCAPES.get(body[index], body[index]))
        elif char == "'":
            assert body[index + 1] == "'", f"unescaped quote in {literal!r}"
            index += 1
            out.append("'")
        else:
            out.append(char)
        index += 1
    return "".join(out)


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
    assert snowflake_reads(string_literal(text)) == text


def test_json_in_a_literal_stays_valid_json() -> None:
    payload = json.dumps({"ground_truth_output": "The reply must state\na count."}, separators=(",", ":"))
    literal = string_literal(payload)
    assert "\\\\n" in literal  # JSON's \n reaches PARSE_JSON as the two characters \ and n
    assert json.loads(snowflake_reads(literal)) == {"ground_truth_output": "The reply must state\na count."}


def test_quote_only_escaping_would_corrupt_the_same_inputs() -> None:
    """The defect this module fixes: escaping only quotes turns `\\t` into a tab."""
    naive = "'" + "C:\\temp".replace("'", "''") + "'"
    assert snowflake_reads(naive) == "C:\temp"
    assert snowflake_reads(string_literal("C:\\temp")) == "C:\\temp"
