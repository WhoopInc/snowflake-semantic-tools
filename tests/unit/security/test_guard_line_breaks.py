"""The guards accept only text every SQL reader splits the same way, and send what they checked.

Readers disagree on which characters end a `--` or `//` comment: Snowflake's lexer may end one
at `\\r` or a Unicode separator where SST's ends it only at `\\n`, which would let text SST read
as comment run as SQL. So the guards refuse every such character, a query's leading comments
are never sent, and canonicalising a statement never changes a guarded fragment in it.
"""

from __future__ import annotations

import unicodedata

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.sql import (
    UnsafeSqlError,
    canonical,
    expr,
    guard_expression,
    guard_query,
    query_text,
    sql,
)
from snowflake_semantic_tools.domain.sql.lexer import LexError, TokenKind, tokenize
from snowflake_semantic_tools.domain.validate.sql import checked_query

# Every control character and Unicode line or paragraph separator, tab and newline included.
CONTROLS = tuple(
    character
    for character in (*map(chr, range(0xA0)), "\u2028", "\u2029")
    if unicodedata.category(character) in ("Cc", "Zl", "Zp")
)
# What `str.splitlines` splits on besides `\n`: a reader that ends a line comment at the
# earliest possible break reads each of these as a newline.
BREAKS = "\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"
AMBIGUOUS = [character for character in CONTROLS if character not in "\t\n"]


def _wrapped(text: str) -> str:
    """The verified-query check validate runs, as the text the driver receives."""
    statement = sql("SELECT COUNT(*) AS ROW_COUNT FROM ({query}) AS SST_VQ", query=query_text(guard_query(text)))
    return str(statement)


def _conservative(text: str) -> list[tuple[TokenKind, int]]:
    """How a reader that takes every break as a newline splits the text."""
    return [(token.kind, token.offset) for token in tokenize(text.translate({ord(c): "\n" for c in BREAKS}))]


def _single_statement(text: str) -> None:
    """Assert text has no comment, `$$` body, or `;` outside strings, as SST and the conservative reader see it."""
    tokens = tokenize(text)
    assert [(token.kind, token.offset) for token in tokens] == _conservative(text)
    assert not [token for token in tokens if token.kind in (TokenKind.COMMENT, TokenKind.DOLLAR_STRING)]
    assert not [token for token in tokens if token.kind is TokenKind.PUNCTUATION and token.text == ";"]


@pytest.mark.parametrize("brk", AMBIGUOUS, ids=repr)
@pytest.mark.parametrize(
    "template",
    [
        "// x{brk}1) AS X; DROP TABLE PROD.S.T; SELECT * FROM (SELECT 1 /*\nSELECT 1 */",
        "-- note{brk}DROP TABLE T /*\nSELECT 1 */",
        "SELECT 1{brk}",
        "SELECT 'a{brk}b'",
    ],
)
def test_the_query_guard_refuses_every_ambiguous_character(template: str, brk: str) -> None:
    with pytest.raises(UnsafeSqlError, match="control character|line break"):
        guard_query(template.format(brk=brk))


@pytest.mark.parametrize("brk", AMBIGUOUS, ids=repr)
def test_the_expression_guard_refuses_every_ambiguous_character(brk: str) -> None:
    for text in (f"a{brk}b", f"'x{brk}'", f'"c{brk}"'):
        with pytest.raises(UnsafeSqlError, match="control character"):
            guard_expression(text)


def test_a_refusal_is_reported_as_unsafe_authored_sql_quoting_the_checked_text() -> None:
    found = checked_query("SELECT 1\r\n\r", kind="verified query", name="q", subject="v")
    assert isinstance(found, Diagnostic)
    assert found.code == "SST-VAL418"
    assert "control character '\\r' is not allowed at '\\r'" in found.message


def test_crlf_is_one_newline_and_trailing_whitespace_goes_before_the_guard_checks() -> None:
    guarded = guard_query("-- header \r\nSELECT a  \r\nFROM t ;  \r\n")
    assert guarded.text == "-- header\nSELECT a\nFROM t ;\n"
    assert guarded.statement == "SELECT a\nFROM t"
    assert guard_expression("CASE WHEN a  \r\nTHEN 'x '\t\nEND").text == "CASE WHEN a\nTHEN 'x '\nEND"


def test_a_leading_comment_is_never_sent() -> None:
    assert _wrapped("/* note */ -- more\n// last\nSELECT 1;") == (
        "SELECT COUNT(*) AS ROW_COUNT FROM (SELECT 1) AS SST_VQ"
    )


def test_a_line_comment_ends_only_at_a_newline() -> None:
    assert [token.kind for token in tokenize("-- a\r\nb")] == [TokenKind.COMMENT, TokenKind.SPACE, TokenKind.WORD]
    assert [token.text for token in tokenize("// a")] == ["// a"]
    for brk in BREAKS:
        with pytest.raises(LexError, match="line break") as raised:
            tokenize(f"x -- a{brk}b\n")
        assert raised.value.offset == 6
    # Inside a string or block comment a break ends nothing, so it lexes.
    assert [token.kind for token in tokenize("'a\rb' /* \u2028 */")] == [
        TokenKind.STRING,
        TokenKind.SPACE,
        TokenKind.COMMENT,
    ]


@pytest.mark.parametrize(
    "text",
    [
        "SELECT SYSTEM$CANCEL_ALL_QUERIES(1)",
        "SELECT system$send_email('i', 'a', 's', 'b')",
        'SELECT "SYSTEM$WAIT"(1)',
        "SELECT x FROM t WHERE SNOWFLAKE.SYSTEM$ABORT_SESSION(1) = 1",
    ],
)
def test_no_system_function_is_accepted(text: str) -> None:
    with pytest.raises(UnsafeSqlError, match="system function"):
        guard_query(text)
    with pytest.raises(UnsafeSqlError, match="system function"):
        guard_expression(text.removeprefix("SELECT "))


def test_a_name_merely_holding_system_is_accepted() -> None:
    assert guard_expression('MY_SYSTEM$X + "a SYSTEM$" + "SYSTEM_X"').text


def test_a_wrapper_altered_after_its_guard_is_refused_where_it_becomes_sql() -> None:
    expression = guard_expression("a")
    object.__setattr__(expression, "text", "a  ")
    with pytest.raises(UnsafeSqlError, match="not the text its guard checked"):
        expr(expression)
    object.__setattr__(expression, "text", "a\rb")
    with pytest.raises(UnsafeSqlError, match="control character"):
        expr(expression)
    query = guard_query("SELECT 1")
    object.__setattr__(query, "statement", "SELECT 1 ")
    with pytest.raises(UnsafeSqlError, match="not the text its guard checked"):
        query_text(query)
    object.__setattr__(query, "statement", "-- x\nSELECT 1")
    with pytest.raises(UnsafeSqlError, match="not the text its guard checked"):
        query_text(query)


# Text with every control character and separator likely, around comment openers and quotes.
_PIECES = st.sampled_from(["--", "//", "/*", "*/", "'", '"', ";", "(", ")", "SELECT", "DROP", " ", "x", *CONTROLS])
_FRAGMENTS = st.lists(_PIECES, max_size=12).map("".join)


@settings(max_examples=400, deadline=None)
@given(_FRAGMENTS, st.sampled_from(["--", "//"]), _FRAGMENTS)
def test_a_query_the_guard_accepts_is_one_statement_however_line_breaks_are_read(
    comment: str, opener: str, tail: str
) -> None:
    """Whatever breaks hide in a leading comment or the query, the sent text is one statement to every reader."""
    text = f"{opener}{comment}\nSELECT 1 {tail}"
    try:
        guarded = guard_query(text)
    except UnsafeSqlError:
        return
    assert not set(guarded.text) & set(AMBIGUOUS)
    _single_statement(guarded.statement)
    _single_statement(_wrapped(text))


@settings(max_examples=400, deadline=None)
@given(_FRAGMENTS)
def test_an_expression_the_guard_accepts_is_one_expression_however_line_breaks_are_read(text: str) -> None:
    try:
        guarded = guard_expression(text)
    except UnsafeSqlError:
        return
    _single_statement(str(sql("SELECT {value} FROM T", value=expr(guarded))))


@settings(max_examples=200, deadline=None)
@given(st.text(alphabet=st.sampled_from(["a", "'", " ", "\t", "\n", "\u3000", "+", "(", ")"]), max_size=30))
def test_canonicalising_a_statement_never_changes_a_guarded_fragment(text: str) -> None:
    """What the guard checked is byte for byte what a canonical statement sends."""
    try:
        guarded = guard_expression(text)
    except UnsafeSqlError:
        return
    for template in (
        sql("SELECT {value}\n  FROM T", value=expr(guarded)),
        sql("X(\n{value}   \n)", value=expr(guarded)),
    ):
        assert guarded.text.strip() in canonical(template).text
