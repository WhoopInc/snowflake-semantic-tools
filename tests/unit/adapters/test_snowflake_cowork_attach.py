"""The connector's reads of the CoWork object (a Snowflake private preview): the statements and rows.

CoWork profiles are a Snowflake private preview; the rows below are shaped as SHOW SNOWFLAKE
INTELLIGENCES and SHOW AGENTS IN SNOWFLAKE INTELLIGENCE return them on an enrolled account.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession, Rows

COWORK = "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"


def _connector(rows: dict[str, Rows]) -> tuple[FakeDriverConnector, FakeDriverSession]:
    session = FakeDriverSession(rows=rows)
    return FakeDriverConnector(session), session


def test_the_cowork_object_is_the_first_one_show_lists_and_none_without_one() -> None:
    connector, session = _connector(
        {"SHOW SNOWFLAKE INTELLIGENCES": ({"name": None}, {"name": COWORK, "owner": "ACCOUNTADMIN"}, {"name": "B"})}
    )
    assert connector.cowork_object() == COWORK
    assert session.statements == ["SHOW SNOWFLAKE INTELLIGENCES"]
    assert _connector({})[0].cowork_object() is None


def test_attached_agents_are_named_from_their_rows_as_show_prints_them() -> None:
    rows: Rows = (
        {"name": "EVERYONE", "database_name": "SNOWFLAKE_INTELLIGENCE", "schema_name": "PROFILES"},
        {"name": "Mixed", "database_name": "DB", "schema_name": "S"},
        {"name": "PARTIAL", "database_name": None, "schema_name": "S"},
    )
    connector, session = _connector({"SHOW AGENTS IN SNOWFLAKE INTELLIGENCE": rows})
    assert connector.attached_agents(COWORK) == (
        QualifiedName.parse("SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"),
        QualifiedName.parse('DB.S."Mixed"'),
    )
    assert session.statements == [f"SHOW AGENTS IN SNOWFLAKE INTELLIGENCE {COWORK}"]
    connector.attached_agents("Quoted")
    assert session.statements[-1] == 'SHOW AGENTS IN SNOWFLAKE INTELLIGENCE "Quoted"'
