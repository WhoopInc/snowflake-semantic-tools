"""The lexer remembers short texts: what it returns is the same whether a text was remembered or not."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.sql import lexer
from snowflake_semantic_tools.domain.sql.lexer import LexError, tokenize


@given(st.text(alphabet=st.sampled_from("ab_$1 .,()'\\\"-/*\n\r$"), max_size=200))
def test_a_remembered_text_lexes_as_a_fresh_lex_of_it_does(text: str) -> None:
    try:
        fresh = lexer._lexed(text)
    except LexError as exc:
        with pytest.raises(LexError) as raised:
            tokenize(text)
        assert (raised.value.offset, raised.value.reason) == (exc.offset, exc.reason)
        return
    assert tokenize(text) == fresh
    assert tokenize(text) == fresh


def test_a_short_text_is_lexed_once_and_a_long_one_every_time() -> None:
    short = "SUM(orders.amount) /* remembered */"
    long = "x" * (lexer._CACHED_LENGTH + 1)

    assert tokenize(short) is tokenize(short)
    assert tokenize(long) is not tokenize(long)
    assert tokenize(long) == lexer._lexed(long)


def test_a_text_that_does_not_lex_raises_every_time() -> None:
    for _ in range(2):
        with pytest.raises(LexError, match="offset 0"):
            tokenize("'never closed")
