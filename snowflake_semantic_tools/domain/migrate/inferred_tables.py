"""`sst migrate refs`: name each table a member's `tables:` lists in the member, or keep the list.

SST 1.0 infers a metric's, filter's or verified query's tables from the `ref()` calls its
expression or SQL makes, so dropping `tables:` loses a table the member names only bare. Before
the key is dropped, each table it lists and the member does not name is written into the member
where that is mechanical:

- a verified query's `FROM`/`JOIN` name of a listed table, bare or qualified, becomes
  `{{ ref('<table>') }}`, its alias kept;
- a metric's or filter's bare column, when `tables:` lists one table, becomes
  `{{ ref('<table>', '<column>') }}`.

A member that calls `metric()` takes its tables from the metrics it calls, so its `tables:`
goes as before. A member that still leaves a listed table unnamed -- a `COUNT(*)`, a second table it reaches
only through `metric()`, SQL in a file, or a scalar this pass cannot rewrite in place -- keeps
its `tables:` and is reported, so a person decides. Every edit stays on its own line, so the
lines `rewrite_member_keys` reads afterwards are the lines this pass read.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.migrate.calls import bare, call
from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.edits import Edit
from snowflake_semantic_tools.domain.migrate.outline import Node
from snowflake_semantic_tools.domain.migrate.semantic import members
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.sql.lexer import LexError, Token, TokenKind, tokenize

_TEMPLATE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_RELATION_WORDS = frozenset(("FROM", "JOIN"))
_NAME_PARTS = (TokenKind.WORD, TokenKind.QUOTED_IDENTIFIER)
# Words an expression uses that are never a column: SQL keywords and literals, date and time
# parts a function takes bare, and the type names a cast names.
_NOT_COLUMNS = frozenset(
    [
        "ALL",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "BETWEEN",
        "BY",
        "CASE",
        "CAST",
        "DESC",
        "DISTINCT",
        "ELSE",
        "END",
        "ESCAPE",
        "EXISTS",
        "FALSE",
        "FILTER",
        "FIRST",
        "FOLLOWING",
        "FROM",
        "IGNORE",
        "ILIKE",
        "IN",
        "INTERVAL",
        "IS",
        "LAST",
        "LIKE",
        "NOT",
        "NULL",
        "NULLS",
        "OR",
        "ORDER",
        "OVER",
        "PARTITION",
        "PRECEDING",
        "QUALIFY",
        "RANGE",
        "RESPECT",
        "RLIKE",
        "ROWS",
        "SOME",
        "THEN",
        "TRUE",
        "TRY_CAST",
        "UNBOUNDED",
        "CURRENT",
        "ROW",
        "WHEN",
        "WITHIN",
        "GROUP",
        "YEAR",
        "YEARS",
        "QUARTER",
        "QUARTERS",
        "MONTH",
        "MONTHS",
        "WEEK",
        "WEEKS",
        "DAY",
        "DAYS",
        "DAYOFWEEK",
        "DAYOFYEAR",
        "HOUR",
        "HOURS",
        "MINUTE",
        "MINUTES",
        "SECOND",
        "SECONDS",
        "MILLISECOND",
        "MILLISECONDS",
        "MICROSECOND",
        "NANOSECOND",
        "EPOCH",
        "BOOLEAN",
        "CHAR",
        "DATE",
        "DATETIME",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INT",
        "INTEGER",
        "NUMBER",
        "NUMERIC",
        "REAL",
        "STRING",
        "TEXT",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
        "VARCHAR",
        "VARIANT",
        "ARRAY",
        "OBJECT",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "BINARY",
    ]
)
# Each member kind with `tables:`, its list root, and the key whose text names its tables.
_KINDS = (
    ("snowflake_metrics", "expr"),
    ("snowflake_filters", "expr"),
    ("snowflake_verified_queries", "sql"),
)


@dataclass(frozen=True, slots=True)
class _Scalar:
    """A scalar's value, and where each of its characters sits in the file.

    Attributes:
        value: The scalar's text as YAML reads it.
        positions: For each character of `value`, its (line, col) in the file.
        single_line: The scalar sits on one line, so writing it double-quoted keeps its lines.
    """

    value: str
    positions: tuple[tuple[int, int], ...]
    single_line: bool


def inferred_tables(lines: Sequence[str], root: Node) -> tuple[Changes, frozenset[int]]:
    """Name in each member the tables its `tables:` lists, or report the member.

    Returns:
        The edits and reports, and the 0-based line of each `tables:` key to keep.
    """
    changes = Changes()
    kept: set[int] = set()
    for list_root, key in _KINDS:
        for member in members(root, list_root):
            _member(lines, member, key, list_root, changes, kept)
    return changes, frozenset(kept)


def _member(lines: Sequence[str], member: Node, key: str, kind: str, changes: Changes, kept: set[int]) -> None:
    """Rewrite one member so it names its listed tables, or keep its `tables:` line and report it.

    Table names compare casefolded, as Snowflake reads an unquoted name.
    """
    entry = member.entry("tables")
    if entry is None or not entry.value.is_seq:
        return
    listed = _listed(entry.value)
    if listed is None:
        return
    target = member.get(key)
    text = target.text or "" if target is not None else ""
    missing = [table for table in listed if table.casefold() not in _named(text)]
    if not missing or _composes(text):
        return
    reason = _rewrite(lines, target, text, listed, missing, kind, changes)
    if reason is not None:
        kept.add(entry.line)
        name = member.get("name")
        changes.leave(entry.line, entry.col, name.text or "" if name is not None else "", reason)


def _listed(tables: Node) -> list[str] | None:
    """Return the models a `tables:` list names, bare or by `ref()`/`table()`; None if any is neither."""
    found: list[str] = []
    for item in tables.items:
        text = item.text or ""
        name = bare(text)
        if name is None:
            try:
                calls = scan_template_calls(text)
            except TemplateSyntaxError:
                return None
            if len(calls) != 1 or calls[0].function not in ("ref", "table") or len(calls[0].args) != 1:
                return None
            name = calls[0].args[0]
        if name.casefold() not in {table.casefold() for table in found}:
            found.append(name)
    return found


def _composes(text: str) -> bool:
    """A member that calls `metric()` takes its tables from the metrics it calls, so SST infers them."""
    try:
        return any(item.function == "metric" for item in scan_template_calls(text))
    except TemplateSyntaxError:
        return False


def _named(text: str) -> set[str]:
    """Return the models a text's `ref()` calls name, casefolded."""
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError:
        return set()
    return {item.args[0].casefold() for item in calls if item.function == "ref" and item.args}


def _rewrite(
    lines: Sequence[str],
    target: Node | None,
    text: str,
    listed: list[str],
    missing: list[str],
    kind: str,
    changes: Changes,
) -> str | None:
    """Write the missing tables into the member; return why it cannot be done, or None when it was."""
    unnamed = ", ".join(missing)
    if target is None or not text.strip():
        return f"it names no table; name {unnamed} with ref(), then delete tables:"
    if "file(" in text:
        return f"its SQL is in a file; name {unnamed} there with ref(), then delete tables:"
    scalar = _scalar(lines, target)
    if scalar is None:
        return f"its {_key(kind)} is a multi-line or escaped scalar; name {unnamed} with ref() by hand"
    try:
        tokens = tokenize(scalar.value)
    except LexError:
        return f"its {_key(kind)} does not lex as SQL; name {unnamed} with ref() by hand"
    if kind == "snowflake_verified_queries":
        splices = list(_relations(tokens, scalar.value, missing))
    elif len(listed) == 1:
        splices = list(_columns(tokens, scalar.value, listed[0]))
    else:
        return f"tables: lists {len(listed)} tables and the expression names {unnamed} nowhere; name each with ref()"
    after = _spliced(scalar.value, splices)
    still = [table for table in missing if table.casefold() not in _named(after)]
    if still:
        hint = f"COUNT({{{{ ref('{still[0]}', '<primary key>') }}}})" if "*" in scalar.value else "ref()"
        return f"its {_key(kind)} names {', '.join(still)} by no column or FROM; write it with {hint}"
    if scalar.single_line:
        quoted = '"' + after.replace("\\", "\\\\").replace('"', '\\"') + '"'
        edit = Edit(target.line, target.col, target.end_line, target.end_col, quoted)
        changes.add(edit, text, after, "ref")
        return None
    if any(scalar.positions[start][0] < 0 or scalar.positions[end - 1][0] < 0 for start, end, _ in splices):
        return f"its {_key(kind)} is a multi-line or escaped scalar; name {unnamed} with ref() by hand"
    for start, end, replacement in reversed(splices):
        line, col = scalar.positions[start]
        end_line, end_col = scalar.positions[end - 1]
        changes.add(Edit(line, col, end_line, end_col + 1, replacement), text[start:end], replacement, "ref")
    return None


def _key(kind: str) -> str:
    return "SQL" if kind == "snowflake_verified_queries" else "expression"


def _significant(tokens: Sequence[Token], index: int, step: int) -> Token | None:
    index += step
    while 0 <= index < len(tokens):
        if tokens[index].kind not in (TokenKind.SPACE, TokenKind.COMMENT):
            return tokens[index]
        index += step
    return None


def _templated(value: str) -> list[tuple[int, int]]:
    return [match.span() for match in _TEMPLATE.finditer(value)]


def _relations(tokens: Sequence[Token], value: str, missing: list[str]) -> Iterator[tuple[int, int, str]]:
    """Yield a splice for each `FROM`/`JOIN` name of a missing table, outside templates.

    The name may be qualified, `db.schema.table`: the whole name becomes the `ref()`.
    """
    wanted = {table.casefold(): table for table in missing}
    spans = _templated(value)
    for index, token in enumerate(tokens):
        if token.kind is not TokenKind.WORD or token.text.casefold() not in wanted:
            continue
        if any(start <= token.offset < end for start, end in spans):
            continue
        first = index
        while first >= 2 and tokens[first - 1].text == "." and tokens[first - 2].kind in _NAME_PARTS:
            first -= 2
        before, following = _significant(tokens, first, -1), _significant(tokens, index, 1)
        if before is None or before.text.upper() not in _RELATION_WORDS:
            continue
        if following is not None and following.text in (".", "("):
            continue
        yield tokens[first].offset, token.offset + len(token.text), call("ref", wanted[token.text.casefold()])


def _columns(tokens: Sequence[Token], value: str, table: str) -> Iterator[tuple[int, int, str]]:
    """Yield a splice for each bare column of the one listed table, outside templates."""
    spans = _templated(value)
    for index, token in enumerate(tokens):
        if token.kind is not TokenKind.WORD or not _IDENTIFIER.fullmatch(token.text):
            continue
        if token.text.upper() in _NOT_COLUMNS or any(start <= token.offset < end for start, end in spans):
            continue
        before, following = _significant(tokens, index, -1), _significant(tokens, index, 1)
        if following is not None and following.text in ("(", "."):
            continue
        if before is not None and (before.text in (".", "::") or before.text.upper() == "AS"):
            continue
        yield token.offset, token.offset + len(token.text), call("ref", table, token.text)


def _spliced(value: str, splices: list[tuple[int, int, str]]) -> str:
    out = value
    for start, end, replacement in reversed(splices):
        out = out[:start] + replacement + out[end:]
    return out


def _scalar(lines: Sequence[str], node: Node) -> _Scalar | None:
    """Locate each character of a scalar in the file; None for a folded or escaped scalar.

    A plain or quoted scalar on one line, and a literal (`|`) or folded (`>`) block, map
    character for character; anything else is left for a person.
    """
    value = node.value
    if value is None:
        return None
    if node.style in ("", "'", '"') and node.line == node.end_line:
        quote = 1 if node.style else 0
        written = lines[node.line][node.col + quote : node.end_col - quote]
        if written != value:
            return None
        positions = tuple((node.line, node.col + quote + index) for index in range(len(value)))
        return _Scalar(value, positions, True)
    if node.style not in ("|", ">"):
        return None
    return _block(lines, node, value)


def _block(lines: Sequence[str], node: Node, value: str) -> _Scalar | None:
    """Map a block scalar's characters to the file, each content line's text found in turn in the value.

    Between two lines a literal block keeps a line break and a folded one may write a space; either
    is skipped, so every character of a line's text maps to where the file writes it. A value the
    lines do not spell out in order is not mapped.
    """
    content = range(node.line + 1, min(node.end_line + 1, len(lines)))
    indent = next(
        (len(lines[number]) - len(lines[number].lstrip(" ")) for number in content if lines[number].strip()), None
    )
    if indent is None:
        return None
    positions: list[tuple[int, int] | None] = [None] * len(value)
    at = 0
    for number in content:
        body = lines[number].rstrip("\r\n")
        if not body.strip():
            continue
        if len(body) - len(body.lstrip(" ")) < indent:
            break
        text = body[indent:]
        while at < len(value) and value[at] in "\n " and not value.startswith(text, at):
            at += 1
        if not value.startswith(text, at):
            return None
        for col in range(len(text)):
            positions[at + col] = (number, indent + col)
        at += len(text)
    if value[at:].strip():
        return None
    return _Scalar(value, tuple(position or (-1, -1) for position in positions), False)
