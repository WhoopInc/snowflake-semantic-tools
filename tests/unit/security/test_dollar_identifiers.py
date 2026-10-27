"""An unquoted name holding `$$` renders quoted, so no reader can take it as opening a `$$` string."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.sql import ident, literal, qname, sql
from snowflake_semantic_tools.domain.sql.lexer import TokenKind, tokenize
from tests.helpers.sql_reference import identifier_names


def test_an_unquoted_name_holding_dollar_dollar_is_quoted_upper_cased() -> None:
    assert ident(Identifier.parse("t$$x")).text == '"T$$X"'
    assert ident(Identifier.shown("T$$")).text == '"T$$"'
    assert qname(QualifiedName.parse("D.S.T$$")).text == 'D.S."T$$"'
    # One `$`, or `$$` already quoted, renders as before.
    assert ident(Identifier.parse("t$x")).text == "T$X"
    assert ident(Identifier("a$$", quoted=True)).text == '"a$$"'


def test_a_dollar_name_beside_a_dollar_literal_stays_one_statement() -> None:
    statement = sql(
        "CREATE VIEW {name} COMMENT = {comment} AS SELECT 1",
        name=qname(QualifiedName.parse("D.S.T$$")),
        comment=literal("$$ AS SELECT 1; DROP TABLE D.S.X; --"),
    )
    kinds = [token.kind for token in tokenize(str(statement))]
    assert TokenKind.DOLLAR_STRING not in kinds and TokenKind.COMMENT not in kinds


@given(st.from_regex(r"[A-Za-z_][A-Za-z0-9_$]{0,20}", fullmatch=True))
def test_every_unquoted_name_still_names_its_upper_cased_text(value: str) -> None:
    rendered = ident(Identifier.parse(value)).text
    assert identifier_names(rendered) == value.upper()
    assert not [token for token in tokenize(rendered) if token.kind is TokenKind.DOLLAR_STRING]
