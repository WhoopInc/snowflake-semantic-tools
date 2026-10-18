"""Find the project relations a routine's body names literally instead of by `ref()` or `source()`.

A literal three-part name reads that one target's relation from every target, and lineage
cannot see it. Only a name equal to the relation a project model, seed, snapshot or source
resolves to is reported, so a routine may still read another team's tables by name. Names are
compared as Snowflake compares them: an unquoted part case-folded to upper, a quoted part exact.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_calls_of
from snowflake_semantic_tools.domain.resolve.calls import KNOWN_FUNCTIONS
from snowflake_semantic_tools.domain.sql.lexer import LexError, TokenKind, tokenize

_UNQUOTED = r"[A-Za-z_][A-Za-z0-9_$]*"
_QUOTED = r'"(?:[^"]|"")+"'
_PART = re.compile(rf"{_UNQUOTED}|{_QUOTED}")
# A dotted name in code that is not SQL. A quoted part may not hold a dot there, so a string
# such as "DB.S.T" in Python is read as the unquoted name inside it.
_CODE_NAME = re.compile(rf'(?<![A-Za-z0-9_$.])(?:{_UNQUOTED}|"[^".]+")(?:\.(?:{_UNQUOTED}|"[^".]+")){{2,}}')

Parts = tuple[str, ...]


def relation_parts(name: str) -> Parts | None:
    """Return a dotted name's parts as Snowflake resolves them; None when it is not a dotted name.

    An unquoted part is upper-cased, and a quoted one keeps its exact text, unquoted.
    """
    parts: list[str] = []
    cursor = 0
    while True:
        found = _PART.match(name, cursor)
        if found is None:
            return None
        parts.append(_folded(found.group()))
        cursor = found.end()
        if cursor == len(name):
            return tuple(parts)
        if name[cursor] != ".":
            return None
        cursor += 1


def _folded(part: str) -> str:
    return part[1:-1].replace('""', '"') if part.startswith('"') else part.upper()


def hardcoded_relations(
    text: str, language: str | None, catalog: DbtCatalog, *, file: str, name: str, subject: str
) -> tuple[Diagnostic, ...]:
    """Report each project relation a body writes by name, once, where it is first written.

    Args:
        text: The body as written, before its template calls are replaced.
        language: The routine's language; `sql` skips names in strings and comments.
        catalog: The target's dbt models and sources, deferral applied.
        file: The body's file, relative to the project root.
        name: The tool member's name.
        subject: The tool member's key.

    Diagnostics:
        SST-VAL626: the body writes a project relation's three-part name.
    """
    calls = _references(catalog)
    if not calls:
        return ()
    masked = _masked(text)
    sql = (language or "").casefold() == "sql"
    found: dict[Parts, int] = {}
    for parts, offset in _names(masked, sql=sql):
        for start in range(len(parts) - 2):
            window = parts[start : start + 3]
            if window in calls and window not in found:
                found[window] = offset
    diagnostics: list[Diagnostic] = []
    for parts, offset in sorted(found.items(), key=lambda item: item[1]):
        line = text.count("\n", 0, offset) + 1
        col = offset - text.rfind("\n", 0, offset)
        diagnostics.append(
            D(
                "SST-VAL626",
                origin=Origin(file, line, col),
                subject=subject,
                name=name,
                relation=".".join(parts),
                expected=calls[parts],
            )
        )
    return tuple(diagnostics)


def _references(catalog: DbtCatalog) -> Mapping[Parts, str]:
    """Map each project relation's parts to the call that names it, the first model's or source's."""
    calls: dict[Parts, str] = {}
    for model in sorted(catalog.models, key=lambda item: item.name.casefold()):
        parts = relation_parts(model.relation_name)
        if parts is not None and len(parts) == 3:
            calls.setdefault(parts, f"{{{{ ref('{model.name}') }}}}")
    for source in catalog.sources:
        parts = relation_parts(source.relation_name or "")
        if parts is not None and len(parts) == 3:
            calls.setdefault(parts, f"{{{{ source('{source.source_name}', '{source.name}') }}}}")
    return calls


def _masked(text: str) -> str:
    """Blank each template call in `text`, keeping its line breaks so every offset still lines up."""
    try:
        calls = scan_calls_of(text, KNOWN_FUNCTIONS)
    except TemplateSyntaxError:
        return text
    masked = text
    for call in calls:
        blank = "".join("\n" if char == "\n" else " " for char in call.raw)
        masked = masked[: call.start] + blank + masked[call.end :]
    return masked


def _names(text: str, *, sql: bool) -> Iterator[tuple[Parts, int]]:
    """Yield each dotted name of three or more parts, with its offset.

    A SQL body is lexed, so a name in a string or a comment is not one; one that does not lex,
    and code in any other language, is matched whole.
    """
    if sql:
        try:
            yield from _sql_names(text)
            return
        except LexError:
            pass
    for found in _CODE_NAME.finditer(text):
        yield tuple(_folded(part) for part in _PART.findall(found.group())), found.start()


def _sql_names(text: str) -> Iterator[tuple[Parts, int]]:
    """Yield each dotted name of three or more parts SQL tokens spell, with its offset.

    Raises:
        LexError: the text does not lex.
    """
    chain: list[str] = []
    start = 0
    expect_part = True
    for token in (*tokenize(text), None):
        if token is not None and expect_part and token.kind in (TokenKind.WORD, TokenKind.QUOTED_IDENTIFIER):
            if not chain:
                start = token.offset
            chain.append(_folded(token.text))
            expect_part = False
            continue
        if token is not None and not expect_part and token.kind is TokenKind.PUNCTUATION and token.text == ".":
            expect_part = True
            continue
        if len(chain) >= 3:
            yield tuple(chain), start
        chain = []
        expect_part = True
        if token is not None and token.kind in (TokenKind.WORD, TokenKind.QUOTED_IDENTIFIER):
            start = token.offset
            chain.append(_folded(token.text))
            expect_part = False
