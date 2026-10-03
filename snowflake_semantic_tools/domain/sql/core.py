"""The `Sql` type and the only two ways to compose it: `sql()` templates and `join()`.

An `Sql` value is text SST may send to Snowflake. Only this package constructs one: the
constructor demands a sentinel private to this module, so code elsewhere composes statements
from the typed constructors in `snowflake_semantic_tools.domain.sql` and never from strings.
A template is static, code-authored text; every value it interpolates is already `Sql`.
"""

from __future__ import annotations

import re
import string
from collections.abc import Iterable
from dataclasses import InitVar, dataclass
from typing import LiteralString

_SEAL = object()
_FORMATTER = string.Formatter()
# A bind placeholder is marked with NUL, which no constructor lets into text: `_seal` refuses it,
# so only a code-authored template can create one, and no value can be read as a placeholder.
_MARK = "\x00"
_PLACEHOLDER = re.compile(r"%(\([A-Za-z_][A-Za-z0-9_]*\))?s")


@dataclass(frozen=True, slots=True)
class Sql:
    """SQL text built only from code-authored templates and values quoted by their constructors.

    Equal when the text is. It is not a `str`: `str(value)` is the one way back to text, and
    the connector takes that step only at the driver boundary.

    Raises:
        TypeError: constructed outside `snowflake_semantic_tools.domain.sql`.
    """

    text: str
    seal: InitVar[object] = None

    def __post_init__(self, seal: object) -> None:
        if seal is not _SEAL:
            raise TypeError("Sql is built only by snowflake_semantic_tools.domain.sql")
        if not isinstance(self.text, str):
            raise TypeError(f"Sql text must be str, found {type(self.text).__name__}")

    def __str__(self) -> str:
        return self.text.replace(_MARK, "%")

    def for_driver(self, *, bound: bool) -> str:
        """Return the text the driver receives, so it reads exactly the statement built.

        The connector formats a statement with Python's `%` operator whenever parameters are
        bound. Bound, every `%` in the text is doubled and only the template's own
        placeholders are restored, so a `%s` inside a literal or a quoted name stays text and
        can never take a bound value. Unbound, the driver formats nothing and the text is sent
        as built.

        Raises:
            ValueError: the statement has a bind placeholder but no parameters are bound.
        """
        if bound:
            return self.text.replace("%", "%%").replace(_MARK, "%")
        if _MARK in self.text:
            raise ValueError("the statement has a bind placeholder but no parameters are bound")
        return self.text


def _seal(text: str) -> Sql:
    """Wrap text this package has already made safe; never call it on outside input.

    Raises:
        ValueError: the text holds the placeholder mark, which only `sql()` may write.
    """
    if isinstance(text, str) and _MARK in text:
        raise ValueError("SQL text may not hold a NUL character")
    return Sql(text, _SEAL)


def _compose(text: str) -> Sql:
    """Wrap text composed from templates and `Sql` parts, which may carry placeholder marks."""
    return Sql(text, _SEAL)


def _mark_placeholders(template: str) -> str:
    """Mark each `%s` and `%(name)s` in a template's static text as a bind placeholder."""
    return _PLACEHOLDER.sub(lambda match: _MARK + match.group(0)[1:], template)


def sql(template: LiteralString, /, **parts: Sql) -> Sql:
    """Fill a static template's `{name}` placeholders with `Sql` parts.

    `{{` and `}}` are literal braces. A placeholder takes no conversion or format spec, may
    repeat, and every part must be named by the template at least once. A `%s` or `%(name)s`
    in the template's own text is a bind placeholder; the same characters inside a part are
    text, and stay text when the statement is bound (see `Sql.for_driver`).

    Example:
        sql("DROP {kind} {name}", kind=keyword("TABLE"), name=qname(table)) gives
        `DROP TABLE DB.S.T`.

    Raises:
        TypeError: the template is not a `str`, or a part is not `Sql`.
        ValueError: a placeholder is malformed or unfilled, or a part is never used.
    """
    if not isinstance(template, str):
        raise TypeError(f"sql() template must be str, found {type(template).__name__}")
    for name, part in parts.items():
        if not isinstance(part, Sql):
            raise TypeError(f"sql() part {name!r} must be Sql, found {type(part).__name__}")
    pieces: list[str] = []
    used: set[str] = set()
    for literal_text, field, spec, conversion in _FORMATTER.parse(template):
        if _MARK in literal_text:
            raise ValueError("sql() template may not hold a NUL character")
        pieces.append(_mark_placeholders(literal_text))
        if field is None:
            continue
        if not field.isidentifier() or spec or conversion is not None:
            raise ValueError(f"sql() placeholder {{{field}}} must be a bare name")
        if field not in parts:
            raise ValueError(f"sql() placeholder {{{field}}} has no part")
        used.add(field)
        pieces.append(parts[field].text)
    unused = sorted(set(parts) - used)
    if unused:
        raise ValueError(f"sql() parts {unused} are not in the template")
    return _compose("".join(pieces))


def join(separator: LiteralString, parts: Iterable[Sql]) -> Sql:
    """Join `Sql` parts with a static separator; no parts give empty text.

    Raises:
        TypeError: the separator is not a `str`, or a part is not `Sql`.
    """
    if not isinstance(separator, str):
        raise TypeError(f"join() separator must be str, found {type(separator).__name__}")
    if _MARK in separator:
        raise ValueError("join() separator may not hold a NUL character")
    texts: list[str] = []
    for part in parts:
        if not isinstance(part, Sql):
            raise TypeError(f"join() part must be Sql, found {type(part).__name__}")
        texts.append(part.text)
    return _compose(separator.join(texts))


def canonical(statement: Sql, *, keep_indent: bool = False) -> Sql:
    """Right-strip every line of a statement and strip the whole, as rendered artifacts store it.

    Only whitespace goes, so the statement keeps its tokens: a quote, bracket, or comment
    marker is never removed.

    Args:
        keep_indent: Strip only the end of the whole, for a fragment that continues a line.
    """
    text = "\n".join(line.rstrip() for line in statement.text.splitlines())
    return _compose(text.rstrip() if keep_indent else text.strip())
