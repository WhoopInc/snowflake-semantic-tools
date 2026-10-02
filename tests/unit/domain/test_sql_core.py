"""The `Sql` type, `sql()` templates, `join()`, and `canonical()`."""

from __future__ import annotations

import dataclasses

import pytest

from snowflake_semantic_tools.domain.sql import Sql, canonical, join, literal, sql
from tests.helpers.sql_values import statement


def test_sql_cannot_be_constructed_outside_the_package() -> None:
    with pytest.raises(TypeError, match="built only by"):
        Sql("DROP TABLE x")
    with pytest.raises(TypeError, match="built only by"):
        Sql("DROP TABLE x", object())
    with pytest.raises(TypeError, match="built only by"):
        dataclasses.replace(sql("SELECT 1"), text="DROP TABLE x")


def test_sql_is_not_a_string_and_converts_only_through_str() -> None:
    value = sql("SELECT 1")
    assert not isinstance(value, str)
    assert str(value) == "SELECT 1"
    assert value == sql("SELECT 1") and value != sql("SELECT 2")
    with pytest.raises(dataclasses.FrozenInstanceError):
        value.text = "DROP"  # type: ignore[misc]


def test_sql_text_must_be_a_string() -> None:
    with pytest.raises(TypeError, match="must be str"):
        statement(b"SELECT 1")  # type: ignore[arg-type]


def test_a_template_fills_each_placeholder_with_sql_parts() -> None:
    assert str(sql("SELECT {value} FROM {value}", value=literal("a"))) == "SELECT 'a' FROM 'a'"
    assert str(sql("{{'literal braces'}}")) == "{'literal braces'}"
    assert str(sql("{{{part}}}", part=sql("x"))) == "{x}"


def test_a_template_refuses_anything_but_sql_parts_and_bare_names() -> None:
    with pytest.raises(TypeError, match="template must be str"):
        sql(b"SELECT 1")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="part 'value' must be Sql"):
        sql("SELECT {value}", value="1; DROP TABLE x")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must be a bare name"):
        sql("SELECT {value!r}", value=sql("1"))
    with pytest.raises(ValueError, match="must be a bare name"):
        sql("SELECT {value:>4}", value=sql("1"))
    with pytest.raises(ValueError, match="must be a bare name"):
        sql("SELECT {value.text}", value=sql("1"))
    with pytest.raises(ValueError, match="has no part"):
        sql("SELECT {missing}")
    with pytest.raises(ValueError, match=r"\['unused'\] are not in the template"):
        sql("SELECT 1", unused=sql("2"))


def test_join_takes_only_sql_parts_and_a_string_separator() -> None:
    assert str(join(", ", (sql("a"), sql("b")))) == "a, b"
    assert str(join(", ", ())) == ""
    with pytest.raises(TypeError, match="separator must be str"):
        join(1, (sql("a"),))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="part must be Sql"):
        join(", ", ("a",))  # type: ignore[arg-type]


def test_canonical_strips_whitespace_only() -> None:
    assert str(canonical(statement("  A  \n  B '  \n'  \n\n"))) == "A\n  B '\n'"
    assert str(canonical(statement("  AS X  \n"), keep_indent=True)) == "  AS X"
