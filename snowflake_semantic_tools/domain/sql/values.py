"""SQL constants: string, number, boolean and NULL literals, typed defaults, dollar quotes, file URIs.

Inside a single-quoted literal Snowflake treats a backslash as an escape character --
'C:\\temp' holds a tab -- and reads a doubled quote as one quote. `literal` escapes both, so
any text survives the round trip, including JSON, whose own escapes are backslashes:
PARSE_JSON of a quote-only-escaped literal fails on the first \\n. A dollar-quoted body has
no escape at all, so `dollar_quoted` refuses any body that could end it early.
"""

from __future__ import annotations

import math
import re
from decimal import Decimal

from snowflake_semantic_tools.domain.sql.core import Sql, _seal, sql
from snowflake_semantic_tools.domain.sql.datatype import datatype, type_family

_DATE = r"\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"
_TIME = r"(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,9})?)?"
_ZONE = r"(?:Z|[+-](?:[01]\d|2[0-3]):?[0-5]\d)?"
# The ISO 8601 text a temporal default is written as, by its type's family.
_TEMPORAL = {
    "date": re.compile(_DATE),
    "time": re.compile(_TIME),
    "timestamp": re.compile(rf"{_DATE}(?:[T ]{_TIME}{_ZONE})?"),
}
# What a default must be, by its type's family, as a refusal names it.
_EXPECTED = {
    "fixed": "a finite number",
    "float": "a finite number",
    "text": "a string",
    "boolean": "true or false",
    "date": "an ISO date such as 2026-01-01",
    "time": "an ISO time such as 13:30:00",
    "timestamp": "an ISO date and time such as 2026-01-01 13:30:00",
    "other": "no default SST can write; use a numeric, text, BOOLEAN, DATE, TIME or TIMESTAMP type",
}


def _unrepresentable(value: str) -> str | None:
    """Name the first character Snowflake cannot hold in a string, or None when there is none."""
    for character in value:
        if character == "\x00":
            return "a NUL character"
        if 0xD800 <= ord(character) <= 0xDFFF:
            return f"the lone surrogate U+{ord(character):04X}"
    return None


def _quoted(value: str) -> str:
    unrepresentable = _unrepresentable(value)
    if unrepresentable is not None:
        raise ValueError(f"a SQL string literal cannot hold {unrepresentable}")
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def literal(value: str) -> Sql:
    r"""Return the single-quoted literal whose value is exactly `value`.

    Newlines stay literal: a multi-line comment spans lines inside one literal.

    Example:
        The text  it's C:\temp  becomes the literal  'it''s C:\\temp'.

    Raises:
        TypeError: `value` is not a `str`.
        ValueError: `value` holds a NUL or a lone surrogate, which no Snowflake string can.
    """
    if not isinstance(value, str):
        raise TypeError(f"literal() takes str, found {type(value).__name__}")
    return _seal(_quoted(value))


def number(value: int | float | Decimal) -> Sql:
    """Return a numeric literal: an integer, a finite float, or a finite decimal.

    Raises:
        TypeError: `value` is a bool or not a number.
        ValueError: `value` is NaN or infinite.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError(f"number() takes int, float or Decimal, found {type(value).__name__}")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"number() cannot render {value!r}")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError(f"number() cannot render {value!r}")
    return _seal(repr(value) if isinstance(value, float) else str(value))


def boolean(value: bool) -> Sql:
    """Return TRUE or FALSE.

    Raises:
        TypeError: `value` is not a bool.
    """
    if not isinstance(value, bool):
        raise TypeError(f"boolean() takes bool, found {type(value).__name__}")
    return _seal("TRUE" if value else "FALSE")


def null() -> Sql:
    """Return NULL."""
    return _seal("NULL")


def dollar_quoted(body: str) -> Sql:
    """Return `$$<body>$$`, refusing a body that could end the quoting early.

    Snowflake has no escape inside dollar quotes, so a body holding `$$`, or ending in `$`
    (which would read `$$$` as its close plus a stray `$`), cannot be quoted.

    Raises:
        ValueError: the body holds `$$`, ends in `$`, or holds a NUL.
    """
    if "$$" in body:
        raise ValueError(f"a dollar-quoted body cannot hold '$$' (at offset {body.index('$$')})")
    if body.endswith("$"):
        raise ValueError("a dollar-quoted body cannot end in '$'")
    if "\x00" in body:
        raise ValueError("a dollar-quoted body cannot hold a NUL character")
    return _seal(f"$${body}$$")


def local_file(path: str) -> Sql:
    """Return the quoted `'file://<path>'` URI that PUT and GET name a local file or directory by.

    The path is written as given, backslashes included, as the client reads it; so it may hold
    no quote, no control character, and no trailing backslash, any of which could end it.

    Raises:
        ValueError: the path is empty or breaks the rule above.
    """
    if not path or "'" in path or path.endswith("\\") or any(ord(character) < 32 for character in path):
        raise ValueError(f"{path!r} cannot be written as a file URI")
    return _seal(f"'file://{path}'")


class DefaultMismatch(ValueError):
    """A variable's default that is not a value of its data type.

    Attributes:
        expected: What the type takes, as a refusal names it.
    """

    def __init__(self, data_type: str, expected: str) -> None:
        super().__init__(f"a {data_type} default must be {expected}")
        self.expected = expected


def typed_default(data_type: str, value: object) -> Sql:
    """Return a variable's default as a constant of its declared data type.

    The type's family decides the value it takes: a number for a numeric type (never a bool),
    true or false for BOOLEAN, a string for text, ISO 8601 text for DATE, TIME and the
    timestamps, and nothing for any other type. A temporal default renders as
    `'<text>'::<data type>`, since Snowflake reads a bare string as VARCHAR.

    Example:
        typed_default("DATE", "2026-01-01") gives `'2026-01-01'::DATE`.

    Raises:
        TypeError: `data_type` is not a `str`.
        ValueError: `data_type` is not a Snowflake data type.
        DefaultMismatch: `value` is not a value of the type; its `expected` says what is.
    """
    family = type_family(data_type)
    refused = DefaultMismatch(data_type.strip(), _EXPECTED[family])
    if family in ("fixed", "float"):
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise refused
        try:
            return number(value)
        except ValueError as exc:
            raise refused from exc
    if family == "boolean":
        if not isinstance(value, bool):
            raise refused
        return boolean(value)
    if not isinstance(value, str) or _unrepresentable(value) is not None:
        raise refused
    if family == "text":
        return literal(value)
    pattern = _TEMPORAL.get(family)
    if pattern is None or not pattern.fullmatch(value.strip()):
        raise refused
    return sql("{text}::{data_type}", text=literal(value.strip()), data_type=datatype(data_type))
