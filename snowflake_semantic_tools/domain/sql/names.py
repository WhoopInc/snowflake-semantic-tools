"""Names as SQL: identifiers, qualified names, stage locations, keywords, and privileges.

An identifier renders through `Identifier.sql`, which double-quotes a quoted name and
doubles any quote inside it; `ident` also checks that an unquoted name really is one, since
`Identifier` itself can be constructed with any text. A keyword or privilege is never
quoted, so each is held to a closed vocabulary.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.sql.core import Sql, _seal, join, sql
from snowflake_semantic_tools.domain.sql.datatype import datatype
from snowflake_semantic_tools.domain.sql.lexer import Token, TokenKind, tokenize
from snowflake_semantic_tools.domain.text import unstorable

_UNQUOTED = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
# Snowflake's reserved and limited keywords: as an identifier each must be quoted or not used.
RESERVED_WORDS = frozenset(
    [
        "ACCOUNT",
        "ALL",
        "ALTER",
        "AND",
        "ANY",
        "AS",
        "BETWEEN",
        "BY",
        "CASE",
        "CAST",
        "CHECK",
        "COLUMN",
        "CONNECT",
        "CONNECTION",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DATABASE",
        "DELETE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXISTS",
        "FALSE",
        "FOLLOWING",
        "FOR",
        "FROM",
        "FULL",
        "GRANT",
        "GROUP",
        "GSCLUSTER",
        "HAVING",
        "ILIKE",
        "IN",
        "INCREMENT",
        "INNER",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "ISSUE",
        "JOIN",
        "LATERAL",
        "LEFT",
        "LIKE",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "MINUS",
        "NATURAL",
        "NOT",
        "NULL",
        "OF",
        "ON",
        "OR",
        "ORDER",
        "ORGANIZATION",
        "QUALIFY",
        "REGEXP",
        "REVOKE",
        "RIGHT",
        "RLIKE",
        "ROW",
        "ROWS",
        "SAMPLE",
        "SCHEMA",
        "SELECT",
        "SET",
        "SOME",
        "START",
        "TABLE",
        "TABLESAMPLE",
        "THEN",
        "TO",
        "TRIGGER",
        "TRUE",
        "TRY_CAST",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VALUES",
        "VIEW",
        "WHEN",
        "WHENEVER",
        "WHERE",
        "WINDOW",
        "WITH",
    ]
)
# Snowflake's limit on an identifier, quoted or not.
_MAX_IDENTIFIER = 255
# The characters a stage path segment may hold unquoted after `@<stage>/`.
_STAGE_SEGMENT = re.compile(r"[A-Za-z0-9_.=$-]+")
_PRIVILEGE_WORD = re.compile(r"[A-Z][A-Z_]*")
_NOT_PRIVILEGE_WORDS = frozenset(("ON", "TO", "FROM", "WITH", "GRANT", "REVOKE", "OPTION"))

# Object types SST names in DDL and SHOW, singular; `keyword(..., plural=True)` adds the S.
OBJECT_TYPES = frozenset(
    (
        "AGENT",
        "CORTEX EXTENSION",
        "CORTEX SEARCH SERVICE",
        "DATASET",
        "FUNCTION",
        "PROCEDURE",
        "SEMANTIC VIEW",
        "STAGE",
        "TABLE",
        "TAG",
        "VIEW",
    )
)
# Every other keyword a statement may take from a value rather than its template.
_KEYWORDS = frozenset(
    (
        *OBJECT_TYPES,
        # Who a grant is to.
        "APPLICATION",
        "APPLICATION ROLE",
        "DATABASE ROLE",
        "ROLE",
        "SHARE",
        "USER",
        # A routine's language and the rights it runs with.
        "JAVA",
        "JAVASCRIPT",
        "PYTHON",
        "SCALA",
        "SQL",
        "CALLER",
        "OWNER",
        "RESTRICTED CALLER",
        # How a search service refreshes.
        "AUTO",
        "FULL",
        "INCREMENTAL",
        # An account-level object SHOW PARAMETERS reads.
        "WAREHOUSE",
    )
)


def _identifier_text(identifier: Identifier) -> str:
    """The text `ident` seals, checked as `ident` documents."""
    if not isinstance(identifier, Identifier):
        raise TypeError(f"expected Identifier, found {type(identifier).__name__}")
    value = identifier.value
    if not value or len(value) > _MAX_IDENTIFIER or unstorable(value) is not None:
        raise ValueError(f"{value!r} is not a Snowflake identifier")
    if not identifier.quoted and not _UNQUOTED.fullmatch(value):
        raise ValueError(f"{value!r} must be quoted to be an identifier")
    # A reader may take `$$` inside a word as opening a dollar-quoted string, and a reserved word
    # as the keyword, so such a name is quoted in the upper case its unquoted form resolves to,
    # naming the same object.
    if not identifier.quoted and ("$$" in value or value.upper() in RESERVED_WORDS):
        return Identifier(value.upper(), quoted=True).sql
    return identifier.sql


def ident(identifier: Identifier) -> Sql:
    """Return one identifier: double-quoted with inner quotes doubled when quoted, else upper-cased.

    An unquoted name holding `$$`, or one that is a Snowflake reserved word such as `select`, is
    written quoted and upper-cased (`"SELECT"`), so no reader can take it as opening a
    dollar-quoted string or as the keyword; it still names the object the unquoted name does.

    Raises:
        TypeError: `identifier` is not an `Identifier`.
        ValueError: it is empty, over 255 characters, holds a NUL or a lone surrogate, or is unquoted but not a
            letter or underscore followed by letters, digits, `_`, or `$`.
    """
    return _seal(_identifier_text(identifier))


def column_ref(table: Identifier, column: Identifier) -> Sql:
    """Return the column reference `TABLE.COLUMN`, each part as `ident` renders it.

    A quoted column keeps its exact case, and an unquoted one that is a reserved word is quoted
    upper-cased, so the reference names the column the table holds.

    Example:
        column_ref(ORDERS, "MixedCaseAmount" quoted) gives `ORDERS."MixedCaseAmount"`, and
        column_ref(ORDERS, order) gives `ORDERS."ORDER"`.

    Raises:
        TypeError: a part is not an `Identifier`.
        ValueError: as `ident` raises it, for either part.
    """
    return _seal(f"{_identifier_text(table)}.{_identifier_text(column)}")


def qname(name: QualifiedName) -> Sql:
    """Return a three-part name, each part as `ident` renders it.

    Raises:
        TypeError: `name` is not a `QualifiedName`.
        ValueError: as `ident` raises it, for any part.
    """
    if not isinstance(name, QualifiedName):
        raise TypeError(f"qname() takes QualifiedName, found {type(name).__name__}")
    return _seal(".".join(_identifier_text(part) for part in (name.database, name.schema, name.name)))


def routine_ref(target: QualifiedName, types: Iterable[str]) -> Sql:
    """Return a procedure or function as Snowflake addresses it: its name, then its argument types.

    A routine with no arguments still takes the parentheses: ALTER, DESCRIBE, GRANT and GET_DDL
    do not parse its bare name.

    Example:
        routine_ref(DB.S.LOOKUP, ("NUMBER", "VARCHAR")) gives `DB.S.LOOKUP(NUMBER, VARCHAR)`, and
        routine_ref(DB.S.COUNT, ()) gives `DB.S.COUNT()`.

    Raises:
        TypeError: `target` is not a `QualifiedName`.
        ValueError: a part of the name is not an identifier, or a type is not a Snowflake type.
    """
    return sql("{target}({types})", target=qname(target), types=join(", ", (datatype(value) for value in types)))


def renamed(statement: Sql, old: QualifiedName, new: QualifiedName) -> Sql:
    """Return `statement` with every reference to the object `old` written as `new` instead.

    A reference is the three parts of `old`, matched as identifiers the way Snowflake folds
    them, joined by dots outside every string, comment and `$$` body; the rest of the text is
    kept as it is. `sst ask` builds a scratch copy of an object this way: the object's own
    statements, naming the copy.
    """
    tokens = list(tokenize(statement.text))
    wanted = old.folded
    out: list[str] = []
    index = 0
    while index < len(tokens):
        found = _name_at(tokens, index)
        if found is not None and found[0] == wanted:
            out.append(qname(new).text)
            index = found[1]
            continue
        out.append(tokens[index].text)
        index += 1
    return _seal("".join(out))


def _name_at(tokens: list[Token], index: int) -> tuple[tuple[str, ...], int] | None:
    """Return the folded parts of a three-part name starting at `index`, and the index after it."""
    parts: list[str] = []
    at = index
    while at < len(tokens) and len(parts) < 3:
        token = tokens[at]
        if token.kind is TokenKind.WORD:
            parts.append(token.text.upper())
        elif token.kind is TokenKind.QUOTED_IDENTIFIER:
            parts.append(token.text[1:-1].replace('""', '"'))
        else:
            return None
        at += 1
        if len(parts) < 3:
            if at >= len(tokens) or tokens[at].text != ".":
                return None
            at += 1
    if (
        len(parts) != 3
        or (index > 0 and tokens[index - 1].text == ".")
        or (at < len(tokens) and tokens[at].text == ".")
    ):
        return None
    return tuple(parts), at


def scope(value: SchemaScope) -> Sql:
    """Return a database and schema as `DATABASE.SCHEMA`.

    Raises:
        TypeError: `value` is not a `SchemaScope`.
        ValueError: as `ident` raises it, for either part.
    """
    if not isinstance(value, SchemaScope):
        raise TypeError(f"scope() takes SchemaScope, found {type(value).__name__}")
    return _seal(f"{_identifier_text(value.database)}.{_identifier_text(value.schema)}")


def stage_path(stage: QualifiedName, path: str = "") -> Sql:
    """Return the unquoted stage location `@<stage>/<path>`, or `@<stage>` for an empty path.

    `path` is `/`-separated segments of letters, digits, `_`, `.`, `=`, `$`, and `-`, none
    of them `.` or `..`; it may end in `/` to name a directory.

    Example:
        stage_path(DB.S.STAGE, "agent/abc123/") gives `@DB.S.STAGE/agent/abc123/`.

    Raises:
        ValueError: a segment is empty, `.` or `..`, or holds any other character.
    """
    location = qname(stage).text
    if not path:
        return _seal(f"@{location}")
    segments = path.removesuffix("/").split("/")
    for segment in segments:
        if segment in (".", "..") or not _STAGE_SEGMENT.fullmatch(segment):
            raise ValueError(f"stage path segment {segment!r} is not safe to write unquoted")
    return _seal(f"@{location}/{path}")


def keyword(value: str, *, plural: bool = False) -> Sql:
    """Return a keyword from SST's closed vocabulary: an object type, grantee kind, or routine word.

    Case and runs of whitespace are normalized; `plural` adds the S that SHOW takes.

    Raises:
        ValueError: the keyword is not in the vocabulary, or `plural` is asked of one that
            is not an object type.
    """
    normalized = " ".join(value.upper().split())
    if normalized not in _KEYWORDS:
        raise ValueError(f"{value!r} is not a keyword SST writes")
    if plural:
        if normalized not in OBJECT_TYPES:
            raise ValueError(f"{value!r} is not an object type")
        return _seal(f"{normalized}S")
    return _seal(normalized)


def privilege(value: str) -> Sql:
    """Return a privilege as SHOW GRANTS names it: one to five words of letters and underscores.

    Upper-cased, with runs of whitespace made one space. No word may be one of the words
    around a privilege in GRANT (ON, TO, WITH, ...), so a privilege cannot extend the statement.

    Raises:
        ValueError: the value is not such a privilege.
    """
    words = value.upper().split()
    if not 1 <= len(words) <= 5 or any(
        not _PRIVILEGE_WORD.fullmatch(word) or word in _NOT_PRIVILEGE_WORDS for word in words
    ):
        raise ValueError(f"{value!r} is not a privilege")
    return _seal(" ".join(words))
