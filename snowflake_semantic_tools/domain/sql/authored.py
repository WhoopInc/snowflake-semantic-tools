"""Authored SQL: expressions and queries a project writes, guarded before anything renders them.

A metric, fact, dimension, or filter expression, a window clause, and a search service's
`where:` are spliced into DDL as written; a verified query runs as written. Each must first
pass a guard here, which returns a wrapper only the guard can build, so a renderer that takes
the wrapper cannot be handed unguarded text.

`guard_expression` admits one expression: no `;`, comment, or `$$` outside strings and quoted
identifiers, balanced brackets, and no statement keyword as a bare word. `guard_query` admits
one SELECT or WITH statement with an optional trailing `;`, leading comments allowed, and no
write or DDL keyword as a bare word.
"""

from __future__ import annotations

from dataclasses import InitVar, dataclass

from snowflake_semantic_tools.domain.sql.core import Sql, _seal
from snowflake_semantic_tools.domain.sql.lexer import LexError, Token, TokenKind, tokenize

_SEAL = object()

# Words that begin or change a statement, refused anywhere outside strings and quoted
# identifiers. DESC is absent: it orders a window or WITHIN GROUP.
STATEMENT_KEYWORDS = frozenset(
    (
        "ALTER",
        "BEGIN",
        "CALL",
        "COMMIT",
        "COPY",
        "CREATE",
        "DELETE",
        "DESCRIBE",
        "DROP",
        "EXECUTE",
        "GRANT",
        "INSERT",
        "LIST",
        "MERGE",
        "PUT",
        "REMOVE",
        "REVOKE",
        "ROLLBACK",
        "SET",
        "SHOW",
        "TRUNCATE",
        "UNDROP",
        "UNSET",
        "UPDATE",
        "USE",
    )
)
# An expression is never a query: no fixture or grammar SST renders needs a subquery in one.
_QUERY_KEYWORDS = frozenset(("SELECT", "WITH", "UNION", "INTERSECT", "MINUS", "EXCEPT"))
# Statement keywords that are also Snowflake functions, allowed when a `(` follows.
FUNCTION_NAMES = frozenset(("INSERT", "TRUNCATE"))
_OPENERS = {"(": ")", "[": "]", "{": "}"}
_CLOSERS = frozenset(_OPENERS.values())


class UnsafeSqlError(ValueError):
    """Authored SQL a guard refused, with where and why.

    Attributes:
        offset: The index into the authored text where the problem starts.
        reason: What is wrong, as a phrase a diagnostic can quote.
    """

    def __init__(self, offset: int, reason: str) -> None:
        super().__init__(f"{reason} at offset {offset}")
        self.offset = offset
        self.reason = reason


@dataclass(frozen=True, slots=True)
class AuthoredExpression:
    """An authored expression `guard_expression` accepted; only the guard builds one.

    Raises:
        TypeError: constructed anywhere but `guard_expression`.
    """

    text: str
    seal: InitVar[object] = None

    def __post_init__(self, seal: object) -> None:
        if seal is not _SEAL:
            raise TypeError("an AuthoredExpression is built only by guard_expression()")


@dataclass(frozen=True, slots=True)
class AuthoredQuery:
    """An authored query `guard_query` accepted; only the guard builds one.

    Attributes:
        text: The query exactly as authored, as a verified query's SQL literal holds it.
        statement: The query as authored up to its trailing `;`, when it has one: what runs.

    Raises:
        TypeError: constructed anywhere but `guard_query`.
    """

    text: str
    statement: str
    seal: InitVar[object] = None

    def __post_init__(self, seal: object) -> None:
        if seal is not _SEAL:
            raise TypeError("an AuthoredQuery is built only by guard_query()")


def _tokens(text: str) -> tuple[Token, ...]:
    if not isinstance(text, str):
        raise TypeError(f"authored SQL must be str, found {type(text).__name__}")
    if not text.strip():
        raise UnsafeSqlError(0, "the SQL is empty")
    try:
        return tokenize(text)
    except LexError as exc:
        raise UnsafeSqlError(exc.offset, exc.reason) from exc


def _next_significant(tokens: tuple[Token, ...], index: int) -> Token | None:
    for token in tokens[index + 1 :]:
        if token.kind is not TokenKind.SPACE:
            return token
    return None


def _refused_word(tokens: tuple[Token, ...], index: int, refused: frozenset[str]) -> str | None:
    """Return the reason a word token is refused, or None when it is allowed where it stands."""
    word = tokens[index].text.upper()
    if word not in refused:
        return None
    following = _next_significant(tokens, index)
    if word in FUNCTION_NAMES and following is not None and following.text == "(":
        return None
    return f"'{tokens[index].text}' is a statement keyword"


def _check(tokens: tuple[Token, ...], refused: frozenset[str], *, start: int, terminator: int | None) -> None:
    """Refuse a comment, `$$`, `;`, refused word, or unbalanced bracket in `tokens[start:]`.

    The token at `terminator` is the one `;` allowed, ending the statement.

    Raises:
        UnsafeSqlError: the first problem found, in text order.
    """
    stack: list[tuple[str, int]] = []
    for index in range(start, len(tokens)):
        token = tokens[index]
        if token.kind is TokenKind.COMMENT:
            raise UnsafeSqlError(token.offset, "a comment is not allowed here")
        if token.kind is TokenKind.DOLLAR_STRING:
            raise UnsafeSqlError(token.offset, "'$$' starts a dollar-quoted string")
        if token.kind is TokenKind.WORD:
            reason = _refused_word(tokens, index, refused)
            if reason is not None:
                raise UnsafeSqlError(token.offset, reason)
        if token.kind is not TokenKind.PUNCTUATION or index == terminator:
            continue
        if token.text == ";":
            raise UnsafeSqlError(token.offset, "';' ends the statement")
        if token.text in _OPENERS:
            stack.append((_OPENERS[token.text], token.offset))
        elif token.text in _CLOSERS:
            if not stack or stack[-1][0] != token.text:
                raise UnsafeSqlError(token.offset, f"'{token.text}' closes nothing")
            stack.pop()
    if stack:
        raise UnsafeSqlError(stack[-1][1], "a bracket is never closed")


def guard_expression(text: str) -> AuthoredExpression:
    """Accept one authored SQL expression, or raise with the offset and reason it is refused.

    Raises:
        TypeError: `text` is not a `str`.
        UnsafeSqlError: the text is empty, does not lex, or holds a comment, `$$`, `;`, an
            unbalanced bracket, or a statement or query keyword outside strings and quoted
            identifiers (INSERT and TRUNCATE are allowed as function calls).
    """
    tokens = _tokens(text)
    _check(tokens, STATEMENT_KEYWORDS | _QUERY_KEYWORDS, start=0, terminator=None)
    return AuthoredExpression(text, _SEAL)


def guard_query(text: str) -> AuthoredQuery:
    """Accept one authored SELECT or WITH query, or raise with the offset and reason it is refused.

    Leading whitespace and comments are allowed; after them the query must begin with SELECT or
    WITH, and may end with one `;`.

    Raises:
        TypeError: `text` is not a `str`.
        UnsafeSqlError: the text is empty or does not lex; it does not begin with SELECT or
            WITH; or, after its first word, it holds a comment, `$$`, a `;` before its end, an
            unbalanced bracket, or a statement keyword outside strings and quoted identifiers.
    """
    tokens = _tokens(text)
    significant = [index for index, token in enumerate(tokens) if token.kind is not TokenKind.SPACE]
    first = next((index for index in significant if tokens[index].kind is not TokenKind.COMMENT), None)
    if first is None or tokens[first].text.upper() not in ("SELECT", "WITH"):
        offset = 0 if first is None else tokens[first].offset
        raise UnsafeSqlError(offset, "a query must begin with SELECT or WITH")
    last = significant[-1]
    terminator = last if tokens[last].text == ";" else None
    _check(tokens, STATEMENT_KEYWORDS, start=first, terminator=terminator)
    statement = text if terminator is None else text[: tokens[terminator].offset]
    return AuthoredQuery(text, statement, _SEAL)


def expr(expression: AuthoredExpression) -> Sql:
    """Return a guarded expression as SQL, exactly as authored.

    Raises:
        TypeError: `expression` is not an `AuthoredExpression`.
    """
    if not isinstance(expression, AuthoredExpression):
        raise TypeError(f"expr() takes AuthoredExpression, found {type(expression).__name__}")
    return _seal(expression.text)


def query_text(query: AuthoredQuery) -> Sql:
    """Return a guarded query's statement as SQL: as authored, without its trailing `;`.

    Raises:
        TypeError: `query` is not an `AuthoredQuery`.
    """
    if not isinstance(query, AuthoredQuery):
        raise TypeError(f"query_text() takes AuthoredQuery, found {type(query).__name__}")
    return _seal(query.statement)
