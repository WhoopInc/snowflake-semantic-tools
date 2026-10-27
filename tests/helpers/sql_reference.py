"""Reference readers for Snowflake SQL, independent of the package's own lexer.

The property tests check the package's output against these: what Snowflake stores for a
single-quoted literal, and what name a double-quoted or bare identifier denotes.
"""

from __future__ import annotations

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


def identifier_names(text: str) -> str:
    """The name one rendered identifier denotes: a quoted one's exact text, a bare one upper-cased."""
    if text.startswith('"'):
        assert text.endswith('"') and len(text) >= 2, text
        inner = text[1:-1]
        assert '"' not in inner.replace('""', ""), f"unescaped quote in {text!r}"
        return inner.replace('""', '"')
    assert text and (text[0].isalpha() or text[0] == "_") and text.isascii(), text
    assert all(char.isalnum() or char in "_$" for char in text), text
    return text.upper()
