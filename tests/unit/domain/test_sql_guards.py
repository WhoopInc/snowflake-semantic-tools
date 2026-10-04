"""The lexer, and the guards that admit authored expressions and queries."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.sql import (
    AuthoredExpression,
    AuthoredQuery,
    UnsafeSqlError,
    expr,
    guard_expression,
    guard_query,
    query_text,
)
from snowflake_semantic_tools.domain.sql.lexer import LexError, TokenKind, tokenize


def kinds(text: str) -> list[tuple[TokenKind, str]]:
    return [(token.kind, token.text) for token in tokenize(text)]


def test_tokens_cover_the_text_and_know_strings_identifiers_and_comments() -> None:
    text = "a.b1 = 'it''s \\' x' -- c\n\"q\"\"d\" /* b */ $$x$$ // e\n12e3 ’"
    assert "".join(token.text for token in tokenize(text)) == text
    assert [kind for kind, _ in kinds(text) if kind is not TokenKind.SPACE] == [
        TokenKind.WORD,
        TokenKind.PUNCTUATION,
        TokenKind.WORD,
        TokenKind.PUNCTUATION,
        TokenKind.STRING,
        TokenKind.COMMENT,
        TokenKind.QUOTED_IDENTIFIER,
        TokenKind.COMMENT,
        TokenKind.DOLLAR_STRING,
        TokenKind.COMMENT,
        TokenKind.NUMBER,
        TokenKind.PUNCTUATION,
    ]
    assert kinds("-- end") == [(TokenKind.COMMENT, "-- end")]
    assert kinds("a \t b")[1] == (TokenKind.SPACE, " \t ")


@pytest.mark.parametrize(
    ("text", "offset", "reason"),
    [
        ("'open", 0, "unterminated string literal"),
        ("x '\\", 2, "unterminated string literal"),
        ('"open', 0, "unterminated quoted identifier"),
        ('"a""', 0, "unterminated quoted identifier"),
        ("$$ body", 0, "unterminated dollar-quoted string"),
        ("/* open", 0, "unterminated block comment"),
        ("a\x00", 1, "NUL character"),
    ],
)
def test_text_that_does_not_lex_names_where_and_why(text: str, offset: int, reason: str) -> None:
    with pytest.raises(LexError) as raised:
        tokenize(text)
    assert (raised.value.offset, raised.value.reason) == (offset, reason)


def test_a_dollar_pair_inside_a_word_starts_a_dollar_string() -> None:
    with pytest.raises(LexError, match="dollar-quoted"):
        tokenize("A$$B")


@pytest.mark.parametrize(
    "text",
    [
        "SUM(ORDERS.ORDER_TOTAL)",
        "COUNT(DISTINCT CASE WHEN A > B THEN C END)",
        "ORDERS.STATE = 'it''s; -- not a comment'",
        "INSERT(A, 1, 2, 'x')",
        "TRUNCATE (A, 2)",
        "LISTAGG(X, ',') WITHIN GROUP (ORDER BY X DESC)",
        "LAG(M) OVER (ORDER BY 1)",
        '"SELECT" + 1',
        "OBJECT_CONSTRUCT('a', 1):a",
        "ARR[0]",
        "{'a': 1}",
        "RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW",
        "COUNT(DISTINCT {{ ref('orders', 'order_id') }})",
    ],
)
def test_a_single_expression_is_accepted_exactly_as_written(text: str) -> None:
    guarded = guard_expression(text)
    assert guarded.text == text
    assert str(expr(guarded)) == text


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("1); DROP TABLE x; --", "')' closes nothing"),
        ("1 ; DROP TABLE x", "';' ends the statement"),
        ("x -- comment", "a comment is not allowed here"),
        ("x /* c */", "a comment is not allowed here"),
        ("a $$ b $$", "'$$' starts a dollar-quoted string"),
        ("(a", "a bracket is never closed"),
        ("(a]", "']' closes nothing"),
        ("a)", "')' closes nothing"),
        ("(SELECT 1)", "'SELECT' is a statement keyword"),
        ("x UNION SELECT secret FROM t", "'UNION' is a statement keyword"),
        ("delete", "'delete' is a statement keyword"),
        ("INSERT INTO t", "'INSERT' is a statement keyword"),
        ("TRUNCATE", "'TRUNCATE' is a statement keyword"),
        ("   ", "the SQL is empty"),
    ],
)
def test_an_expression_that_could_escape_its_place_is_refused(text: str, reason: str) -> None:
    with pytest.raises(UnsafeSqlError) as raised:
        guard_expression(text)
    assert raised.value.reason == reason
    assert str(raised.value).startswith(reason)


def test_guards_take_only_text_and_report_lex_errors_as_refusals() -> None:
    with pytest.raises(TypeError, match="must be str"):
        guard_expression(None)  # type: ignore[arg-type]
    with pytest.raises(UnsafeSqlError, match="unterminated string literal at offset 0"):
        guard_expression("' OR 1=1 --")


@pytest.mark.parametrize(
    ("text", "statement"),
    [
        ("SELECT 1", "SELECT 1"),
        ("SELECT 1;", "SELECT 1"),
        ("SELECT 1 ;\n", "SELECT 1"),
        (
            "-- header\n/* note */\nWITH a AS (SELECT 1) SELECT * FROM a",
            "WITH a AS (SELECT 1) SELECT * FROM a",
        ),
        ("select 'a;b' from t", "select 'a;b' from t"),
        ("SELECT INSERT(a, 1, 1, 'x') FROM t", "SELECT INSERT(a, 1, 1, 'x') FROM t"),
    ],
)
def test_one_read_query_is_accepted_and_its_terminator_dropped(text: str, statement: str) -> None:
    guarded = guard_query(text)
    assert guarded.text == text
    assert guarded.statement == statement
    assert str(query_text(guarded)) == statement


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("SELECT 1; DROP TABLE x", "';' ends the statement"),
        ("SELECT 1;;", "';' ends the statement"),
        ("DELETE FROM x", "a query must begin with SELECT or WITH"),
        ("-- only a comment", "a query must begin with SELECT or WITH"),
        ("SELECT 1 -- trailing", "a comment is not allowed here"),
        ("SELECT * FROM t WHERE x IN (SELECT 1", "a bracket is never closed"),
        ("WITH d AS (DELETE FROM t) SELECT 1", "'DELETE' is a statement keyword"),
        ("SELECT $$x$$", "'$$' starts a dollar-quoted string"),
        ("", "the SQL is empty"),
    ],
)
def test_anything_but_one_read_query_is_refused(text: str, reason: str) -> None:
    with pytest.raises(UnsafeSqlError) as raised:
        guard_query(text)
    assert raised.value.reason == reason


def test_guarded_values_are_built_only_by_their_guards() -> None:
    with pytest.raises(TypeError, match="guard_expression"):
        AuthoredExpression("1; DROP TABLE x")
    with pytest.raises(TypeError, match="guard_query"):
        AuthoredQuery("DROP TABLE x", "DROP TABLE x")
    with pytest.raises(TypeError, match="AuthoredExpression"):
        expr("1")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="AuthoredQuery"):
        query_text("SELECT 1")  # type: ignore[arg-type]


def test_an_ambiguous_line_break_or_system_function_is_refused() -> None:
    with pytest.raises(UnsafeSqlError, match="control character"):
        guard_query("SELECT 1\rFROM t")
    with pytest.raises(LexError, match="line break"):
        tokenize("-- c\x0bx")
    with pytest.raises(UnsafeSqlError, match="is a system function"):
        guard_expression("SYSTEM$CANCEL_QUERY(1)")
    with pytest.raises(UnsafeSqlError, match="names a system function"):
        guard_query('SELECT "system$cancel_query"(1)')


def test_a_wrapper_altered_after_its_guard_is_refused_as_sql() -> None:
    expression = guard_expression("a")
    object.__setattr__(expression, "text", "a  ")
    with pytest.raises(UnsafeSqlError, match="not the text its guard checked"):
        expr(expression)
    query = guard_query("SELECT 1")
    object.__setattr__(query, "statement", "SELECT 1 ")
    with pytest.raises(UnsafeSqlError, match="not the text its guard checked"):
        query_text(query)
