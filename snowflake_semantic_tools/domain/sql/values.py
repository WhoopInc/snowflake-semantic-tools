"""SQL constants: string, number, boolean, and NULL literals, dollar-quoted bodies, and file URIs.

Inside a single-quoted literal Snowflake treats a backslash as an escape character --
'C:\\temp' holds a tab -- and reads a doubled quote as one quote. `literal` escapes both, so
any text survives the round trip, including JSON, whose own escapes are backslashes:
PARSE_JSON of a quote-only-escaped literal fails on the first \\n. A dollar-quoted body has
no escape at all, so `dollar_quoted` refuses any body that could end it early.
"""

from __future__ import annotations

import math
from decimal import Decimal

from snowflake_semantic_tools.domain.sql.core import Sql, _seal


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


def bound_literal(value: str) -> Sql:
    """Return `literal(value)` with each `%` doubled, for a statement the driver binds with `%s`.

    The driver formats such a statement with Python's `%` operator, which reads `%%` as one
    percent sign, so the literal reaches Snowflake as `literal` would write it.

    Raises:
        TypeError: as `literal` raises it.
        ValueError: as `literal` raises it.
    """
    return _seal(literal(value).text.replace("%", "%%"))


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
