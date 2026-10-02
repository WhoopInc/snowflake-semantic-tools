"""Names as SQL: identifiers, qualified names, stage locations, keywords, privileges, and types."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.sql import (
    datatype,
    ident,
    is_datatype,
    keyword,
    privilege,
    qname,
    scope,
    stage_path,
)

NAME = QualifiedName.parse('db.s."we""ird"')


def test_identifiers_render_quoted_exactly_or_unquoted_upper_cased() -> None:
    assert str(ident(Identifier.parse("orders"))) == "ORDERS"
    assert str(ident(Identifier.parse('"Mixed ""Case"""'))) == '"Mixed ""Case"""'
    assert str(qname(NAME)) == 'DB.S."we""ird"'
    assert str(scope(SchemaScope.from_qualified_name(NAME))) == "DB.S"


@pytest.mark.parametrize(
    "identifier",
    [
        Identifier("a b"),
        Identifier("x; DROP TABLE t"),
        Identifier(""),
        Identifier("", quoted=True),
        Identifier("a\x00", quoted=True),
        Identifier("a\ud800", quoted=True),
        Identifier("x" * 256, quoted=True),
    ],
)
def test_an_identifier_that_cannot_render_safely_is_refused(identifier: Identifier) -> None:
    with pytest.raises(ValueError, match="identifier|quoted"):
        ident(identifier)


def test_name_constructors_refuse_other_types() -> None:
    with pytest.raises(TypeError, match="expected Identifier"):
        ident("ORDERS")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="QualifiedName"):
        qname("DB.S.T")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="SchemaScope"):
        scope("DB.S")  # type: ignore[arg-type]


def test_a_stage_location_takes_only_safe_segments() -> None:
    assert str(stage_path(NAME)) == '@DB.S."we""ird"'
    assert str(stage_path(NAME, "agent/abc123/")) == '@DB.S."we""ird"/agent/abc123/'
    assert str(stage_path(NAME, "a=1/SST_$X.yaml")) == '@DB.S."we""ird"/a=1/SST_$X.yaml'
    for bad in ("../x", "a//b", "a/./b", "a b/", "a'/", "a;DROP/", "/abs"):
        with pytest.raises(ValueError, match="not safe"):
            stage_path(NAME, bad)


def test_keywords_come_from_a_closed_vocabulary() -> None:
    assert str(keyword("semantic   view")) == "SEMANTIC VIEW"
    assert str(keyword("Cortex Search Service", plural=True)) == "CORTEX SEARCH SERVICES"
    assert str(keyword("restricted caller")) == "RESTRICTED CALLER"
    with pytest.raises(ValueError, match="not a keyword"):
        keyword("TABLE; DROP TABLE x")
    with pytest.raises(ValueError, match="not an object type"):
        keyword("ROLE", plural=True)


def test_privileges_are_words_that_cannot_extend_a_grant() -> None:
    assert str(privilege("select")) == "SELECT"
    assert str(privilege("evolve   schema")) == "EVOLVE SCHEMA"
    for bad in ("", "SELECT ON TABLE X TO ROLE R", "USAGE; DROP", "A B C D E F", "WITH GRANT OPTION", "READ1"):
        with pytest.raises(ValueError, match="not a privilege"):
            privilege(bad)


@pytest.mark.parametrize(
    "text",
    [
        "NUMBER",
        "number(38, 2)",
        "NUMBER(38)",
        "DECIMAL ( 10 , 0 )",
        "INT",
        "DOUBLE PRECISION",
        "VARCHAR(16777216)",
        "char varying(10)",
        "TIMESTAMP_NTZ(9)",
        "TIMESTAMP_TZ",
        "VARIANT",
        "GEOGRAPHY",
        "VECTOR(FLOAT, 256)",
        "vector(int,3)",
        "  BOOLEAN  ",
    ],
)
def test_data_types_that_match_the_grammar_render_as_written(text: str) -> None:
    assert str(datatype(text)) == text.strip()
    assert is_datatype(text)


@pytest.mark.parametrize(
    "text",
    ["", "NUMBER(38,2) NOT NULL", "VARCHAR); DROP TABLE x; --", "ARRAY(NUMBER)", "VECTOR(TEXT, 3)", "INT(4)", "MAP"],
)
def test_data_types_outside_the_grammar_are_refused(text: str) -> None:
    with pytest.raises(ValueError, match="not a Snowflake data type"):
        datatype(text)
    assert not is_datatype(text)


def test_a_data_type_must_be_text() -> None:
    with pytest.raises(TypeError, match="takes str"):
        datatype(3)  # type: ignore[arg-type]
    assert not is_datatype(3)  # type: ignore[arg-type]
