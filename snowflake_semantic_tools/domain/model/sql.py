r"""SQL string literals that Snowflake reads back as exactly the text they were built from.

Inside a single-quoted literal Snowflake treats a backslash as an escape character --
'C:\temp' holds a tab -- and reads a doubled quote as one quote. `string_literal`
escapes both, so any text survives the round trip, including JSON, whose own escapes
are backslashes: PARSE_JSON of a quote-only-escaped literal fails on the first \n.
Every renderer and adapter that writes a string literal uses this module.
"""

from __future__ import annotations


def string_literal(value: str) -> str:
    r"""Return the single-quoted Snowflake literal whose value is exactly `value`.

    Newlines stay literal: a multi-line comment spans lines inside one literal.

    Example:
        The text  it's C:\temp  becomes the literal  'it''s C:\\temp'.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
