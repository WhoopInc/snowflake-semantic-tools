"""What authored text may hold, and how SST trims the text it publishes.

Snowflake stores text as UTF-8 and cannot hold a NUL or a lone UTF-16 surrogate in a string,
an identifier or a dollar-quoted body. `unstorable` names the first such character in one
string, and `unstorable_strings` finds every string in a loaded tree that holds one, so a
reader can refuse it where it is written rather than at render. `comment_text` is the one
rule a published comment follows: its ends are trimmed and everything between them is kept
exactly, line breaks, carriage returns and inner spaces among it.
"""

from __future__ import annotations

from collections.abc import Iterator

TreePath = tuple[str | int, ...]


def unstorable(value: str) -> str | None:
    """Name the first character of `value` Snowflake cannot store, or None when there is none."""
    for character in value:
        if character == "\x00":
            return "a NUL character"
        if 0xD800 <= ord(character) <= 0xDFFF:
            return f"the lone surrogate U+{ord(character):04X}"
    return None


def unstorable_strings(value: object, path: TreePath = ()) -> Iterator[tuple[TreePath, str]]:
    """Yield the path and problem of each string `unstorable` names in a tree of lists and mappings.

    A mapping key is checked as well as its value; a key that cannot be stored is reported at
    the path of the mapping holding it, its value at the path the key extends. Strings are
    visited in document order: mappings by insertion, lists by index.
    """
    if isinstance(value, str):
        problem = unstorable(value)
        if problem is not None:
            yield path, problem
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from unstorable_strings(item, (*path, index))
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from unstorable_strings(key, path)
            yield from unstorable_strings(item, (*path, str(key)))


def comment_text(value: str | None) -> str:
    """Return the text a published comment carries: `value` with its ends trimmed, else unchanged."""
    return (value or "").strip()
