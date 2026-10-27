"""What authored text may hold, and how a published comment is trimmed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.lifecycle import marked_comment
from snowflake_semantic_tools.domain.sql import canonical, literal, sql
from snowflake_semantic_tools.domain.text import comment_text, unstorable, unstorable_strings


def test_unstorable_names_the_first_nul_or_lone_surrogate() -> None:
    assert unstorable("plain \u00e9 \U0001f600 text") is None
    assert unstorable("a\x00b\ud800") == "a NUL character"
    assert unstorable("a\udfffb") == "the lone surrogate U+DFFF"


def test_unstorable_strings_finds_keys_and_values_by_their_path() -> None:
    tree = {"a": ["ok", "x\x00"], "k\ud800": {"b": "\ud800"}, "n": 3}
    assert list(unstorable_strings(tree)) == [
        (("a", 1), "a NUL character"),
        ((), "the lone surrogate U+D800"),
        (("k\ud800", "b"), "the lone surrogate U+D800"),
    ]
    assert list(unstorable_strings({"a": ["ok"], "b": None})) == []


def test_comment_text_trims_the_ends_and_keeps_everything_between() -> None:
    assert comment_text("  Use it \r\nfor x.\t ") == "Use it \r\nfor x."
    assert comment_text(None) == comment_text("   ") == ""


def test_a_published_comment_keeps_its_carriage_returns_and_inner_spaces() -> None:
    comment = marked_comment(" Use it \r\nfor x. ", "[m]")
    assert comment == "Use it \r\nfor x. [m]"
    statement = canonical(sql("ALTER PROCEDURE P() SET COMMENT = {comment}  \n", comment=literal(comment)))
    assert str(statement) == "ALTER PROCEDURE P() SET COMMENT = 'Use it \r\nfor x. [m]'"
