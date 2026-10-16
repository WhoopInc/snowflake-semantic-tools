"""Snowflake data types, accepted only when they match Snowflake's type grammar, and their families.

A type is written into DDL unquoted, so it is held to the grammar here rather than passed
through: a base type name, case-insensitive, with the parameters that type takes. A type's
family groups the synonyms Snowflake treats alike, which is what decides the values it holds.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.sql.core import Sql, _seal

_N = r"\s*\d{1,9}\s*"
_GRAMMAR = re.compile(
    "|".join(
        (
            # Fixed-point: no parameters, a precision, or a precision and a scale.
            rf"(?:NUMBER|DECIMAL|DEC|NUMERIC)(?:\s*\({_N}(?:,{_N})?\))?",
            r"INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT",
            r"FLOAT|FLOAT4|FLOAT8|DOUBLE\s+PRECISION|DOUBLE|REAL",
            # Text and binary: an optional length.
            rf"(?:VARCHAR|CHAR\s+VARYING|CHARACTER|CHAR|NCHAR\s+VARYING|NCHAR|NVARCHAR2|NVARCHAR"
            rf"|STRING|TEXT|BINARY|VARBINARY)(?:\s*\({_N}\))?",
            r"BOOLEAN|DATE|VARIANT|OBJECT|ARRAY|GEOGRAPHY|GEOMETRY",
            # Time and timestamps: an optional fractional-seconds precision.
            rf"(?:DATETIME|TIME|TIMESTAMP_LTZ|TIMESTAMP_NTZ|TIMESTAMP_TZ|TIMESTAMP)(?:\s*\({_N}\))?",
            rf"VECTOR\s*\(\s*(?:INT|FLOAT)\s*,{_N}\)",
        )
    ),
    re.IGNORECASE,
)


# Each base type name, as `_BASE` reads it, by its family; every other type is `other`.
_FAMILIES = {
    **dict.fromkeys(
        ("NUMBER", "DECIMAL", "DEC", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"),
        "fixed",
    ),
    **dict.fromkeys(("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE PRECISION", "DOUBLE", "REAL"), "float"),
    **dict.fromkeys(
        (
            "VARCHAR",
            "CHAR VARYING",
            "CHARACTER",
            "CHAR",
            "NCHAR VARYING",
            "NCHAR",
            "NVARCHAR2",
            "NVARCHAR",
            "STRING",
            "TEXT",
        ),
        "text",
    ),
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIME": "time",
    **dict.fromkeys(("DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"), "timestamp"),
}
_BASE = re.compile(r"[A-Z_0-9]+(?:\s+(?:PRECISION|VARYING))?")


def datatype(value: str) -> Sql:
    """Return a Snowflake data type exactly as written, once it matches the type grammar.

    Surrounding whitespace is dropped and nothing else changes, so `number(38, 2)` renders
    as written. Structured ARRAY, OBJECT, and MAP types are not accepted.

    Raises:
        TypeError: `value` is not a `str`.
        ValueError: `value` is not a Snowflake data type.
    """
    if not isinstance(value, str):
        raise TypeError(f"datatype() takes str, found {type(value).__name__}")
    text = value.strip()
    if not _GRAMMAR.fullmatch(text):
        raise ValueError(f"{value!r} is not a Snowflake data type")
    return _seal(text)


def is_datatype(value: str) -> bool:
    """Report whether `datatype(value)` would accept `value`."""
    return isinstance(value, str) and _GRAMMAR.fullmatch(value.strip()) is not None


def type_family(value: str) -> str:
    """Return the family of a Snowflake data type, which decides the values it holds.

    The families are `fixed` (NUMBER and its synonyms), `float`, `text`, `boolean`, `date`,
    `time`, `timestamp` (DATETIME and every TIMESTAMP), and `other` for the rest: binary,
    semi-structured, geospatial, and vector types.

    Raises:
        TypeError: `value` is not a `str`.
        ValueError: `value` is not a Snowflake data type.
    """
    text = datatype(value).text.upper()
    match = _BASE.match(text)
    assert match is not None
    return _FAMILIES.get(" ".join(match.group().split()), "other")
