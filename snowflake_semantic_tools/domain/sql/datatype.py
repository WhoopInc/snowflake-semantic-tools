"""Snowflake data types, accepted only when they match Snowflake's type grammar.

A type is written into DDL unquoted, so it is held to the grammar here rather than passed
through: a base type name, case-insensitive, with the parameters that type takes.
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
