"""Names as SQL: identifiers, qualified names, stage locations, keywords, and privileges.

An identifier renders through `Identifier.sql`, which double-quotes a quoted name and
doubles any quote inside it; `ident` also checks that an unquoted name really is one, since
`Identifier` itself can be constructed with any text. A keyword or privilege is never
quoted, so each is held to a closed vocabulary.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.sql.core import Sql, _seal
from snowflake_semantic_tools.domain.sql.values import _unrepresentable

_UNQUOTED = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
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
    if not isinstance(identifier, Identifier):
        raise TypeError(f"expected Identifier, found {type(identifier).__name__}")
    value = identifier.value
    if not value or len(value) > _MAX_IDENTIFIER or _unrepresentable(value) is not None:
        raise ValueError(f"{value!r} is not a Snowflake identifier")
    if not identifier.quoted and not _UNQUOTED.fullmatch(value):
        raise ValueError(f"{value!r} must be quoted to be an identifier")
    return identifier.sql


def ident(identifier: Identifier) -> Sql:
    """Return one identifier: double-quoted with inner quotes doubled when quoted, else upper-cased.

    Raises:
        TypeError: `identifier` is not an `Identifier`.
        ValueError: it is empty, over 255 characters, holds a NUL or a lone surrogate, or is unquoted but not a
            letter or underscore followed by letters, digits, `_`, or `$`.
    """
    return _seal(_identifier_text(identifier))


def qname(name: QualifiedName) -> Sql:
    """Return a three-part name, each part as `ident` renders it.

    Raises:
        TypeError: `name` is not a `QualifiedName`.
        ValueError: as `ident` raises it, for any part.
    """
    if not isinstance(name, QualifiedName):
        raise TypeError(f"qname() takes QualifiedName, found {type(name).__name__}")
    return _seal(".".join(_identifier_text(part) for part in (name.database, name.schema, name.name)))


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
