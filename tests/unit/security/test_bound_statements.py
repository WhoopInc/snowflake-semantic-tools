"""Bound statements reach Snowflake exactly as built, whatever their text holds.

The connector binds parameters on the client: it formats the whole statement with Python's
`%` operator. Text inside a literal or a quoted name that looks like a placeholder must stay
text, or a bound value could land inside a literal and close it. These tests format with the
connector's own code path, offline, so they check what Snowflake would actually be sent.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.converter import SnowflakeConverter

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.sql import Sql, ident, join, literal, qname, sql

TABLE = QualifiedName.parse("DB.S.T")
HOSTILE = "zz'; DROP TABLE DB.S.T; --"
# The connector quotes a bound string itself, escaping its quote with a backslash.
BOUND = "'zz\\'; DROP TABLE DB.S.T; --'"


def _driver_text(statement: Sql, params: Any) -> str:
    """Return the text the connector sends: its pyformat processing, applied as it applies it."""
    # A bare connection holds only what pyformat processing reads; nothing connects.
    connection: Any = SnowflakeConnection.__new__(SnowflakeConnection)
    connection.converter = SnowflakeConverter()
    connection._interpolate_empty_sequences = False
    processed = SnowflakeConnection._process_params_pyformat(connection, params)
    text = statement.for_driver(bound=params is not None)
    # As `SnowflakeCursor._preprocess_pyformat_query`: nothing bound, nothing formatted.
    return str(text % processed) if processed else str(text)


@pytest.mark.parametrize("authored", ["x%s, 'y", "%(name)s", "100%", "%%", "a%sb%(x)sc"])
def test_a_placeholder_in_a_literal_stays_text_and_takes_no_bound_value(authored: str) -> None:
    statement = sql("UPDATE {t} SET NOTE = {note} WHERE NAME = %s", t=qname(TABLE), note=literal(authored))
    sent = _driver_text(statement, (HOSTILE,))
    assert sent == f"UPDATE DB.S.T SET NOTE = {literal(authored)} WHERE NAME = {BOUND}"


def test_a_placeholder_in_a_quoted_name_stays_text() -> None:
    table = QualifiedName(Identifier("DB"), Identifier("S"), Identifier("t%s", quoted=True))
    sent = _driver_text(sql("SELECT * FROM {t} WHERE NAME = %s", t=qname(table)), (HOSTILE,))
    assert sent == f'SELECT * FROM DB.S."t%s" WHERE NAME = {BOUND}'


def test_named_placeholders_survive_composition_and_bind_by_name() -> None:
    fragment = sql("PARSE_JSON(%(hooks)s)")
    statement = sql("SELECT {value}, {note}", value=join(", ", (fragment,)), note=literal("%(hooks)s"))
    assert _driver_text(statement, {"hooks": HOSTILE}) == (f"SELECT PARSE_JSON({BOUND}), '%(hooks)s'")


def test_an_unbound_statement_is_sent_as_built() -> None:
    statement = sql("COMMENT ON TABLE {t} IS {note}", t=qname(TABLE), note=literal("50% off, %s"))
    assert _driver_text(statement, None) == "COMMENT ON TABLE DB.S.T IS '50% off, %s'"


def test_a_statement_with_a_placeholder_refuses_to_run_unbound() -> None:
    with pytest.raises(ValueError, match="no parameters are bound"):
        sql("SELECT * FROM {t} WHERE NAME = %s", t=qname(TABLE)).for_driver(bound=False)


def test_no_value_can_forge_a_placeholder() -> None:
    with pytest.raises(ValueError):
        literal("\x00s")
    with pytest.raises(ValueError):
        ident(Identifier("a\x00s", quoted=True))
    with pytest.raises(ValueError):
        join("\x00s", ())


@given(st.text(alphabet=st.characters(blacklist_categories=["Cs"], blacklist_characters="\x00")))
def test_any_authored_text_reaches_snowflake_unchanged_in_a_bound_statement(authored: str) -> None:
    statement = sql("SELECT {note} WHERE NAME = %s", note=literal(authored))
    assert _driver_text(statement, (HOSTILE,)) == f"SELECT {literal(authored)} WHERE NAME = {BOUND}"
