"""The connector's preflight reads: what each SHOW or query it sends returns to plan."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.sql import Sql

SCOPE = SchemaScope(Identifier.parse("DB"), Identifier.parse("SCH"))


class ScriptedConnector(SnowflakeConnector):
    """A connector answering each statement from the rows scripted for its prefix, recording what it sent."""

    def __init__(
        self,
        rows: Mapping[str, tuple[Mapping[str, object], ...]] | None = None,
        results: Mapping[str, QueryResult] | None = None,
    ) -> None:
        self._rows = {prefix: tuple(dict(row) for row in found) for prefix, found in (rows or {}).items()}
        self._results = dict(results or {})
        self.sent: list[tuple[str, object]] = []

    def _dict_rows(self, sql: Sql) -> tuple[dict[str, object], ...]:
        self.sent.append((str(sql), None))
        return next((rows for prefix, rows in self._rows.items() if str(sql).startswith(prefix)), ())

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        self.sent.append((str(sql), params))
        return next(
            (result for prefix, result in self._results.items() if str(sql).startswith(prefix)), QueryResult((), ())
        )

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        self.sent.append((object_type, qualified_name.sql))
        return qualified_name.name.folded == "ORDERS"


def test_containers_and_warehouses_exist_only_when_show_lists_their_exact_name() -> None:
    connector = ScriptedConnector(
        {
            "SHOW DATABASES": ({"name": "DB"}, {"name": "DB_OLD"}),
            "SHOW SCHEMAS": ({"name": "SCH"},),
            "SHOW WAREHOUSES": ({"name": "WH_2"},),
        }
    )
    assert connector.database_exists(Identifier.parse("db"))
    assert not connector.database_exists(Identifier.parse("other"))
    assert connector.schema_exists(SCOPE)
    assert not connector.warehouse_exists(Identifier.parse("WH"))
    assert connector.sent[0] == ("SHOW DATABASES LIKE 'DB'", None)
    assert connector.sent[2] == ("SHOW SCHEMAS LIKE 'SCH' IN DATABASE DB", None)


def test_a_relation_is_a_table_or_a_view() -> None:
    connector = ScriptedConnector()
    assert connector.relation_exists(QualifiedName.parse("DB.SCH.ORDERS"))
    assert connector.sent == [("TABLE OR VIEW", "DB.SCH.ORDERS")]


def test_a_privilege_is_held_by_a_role_in_the_session_that_is_granted_it_or_owns_the_schema() -> None:
    grants: tuple[dict[str, object], ...] = (
        {"privilege": "CREATE SEMANTIC VIEW", "granted_to": "ROLE", "grantee_name": "BUILDER"},
        {"privilege": "OWNERSHIP", "granted_to": "ROLE", "grantee_name": "OWNER"},
        {"privilege": "CREATE AGENT", "granted_to": "DATABASE_ROLE", "grantee_name": "DB.AGENTS"},
    )
    connector = ScriptedConnector(
        {"SHOW GRANTS ON SCHEMA": grants}, {"SELECT IS_ROLE_IN_SESSION": QueryResult(("IN",), ((False,),))}
    )
    privileges = ("CREATE SEMANTIC VIEW", "CREATE AGENT")
    assert connector.missing_privileges(SCOPE, privileges) == privileges
    role_checks = [params for statement, params in connector.sent if statement.startswith("SELECT IS_ROLE")]
    assert role_checks == [("BUILDER",), ("OWNER",)]

    owner = ScriptedConnector(
        {"SHOW GRANTS ON SCHEMA": grants}, {"SELECT IS_ROLE_IN_SESSION": QueryResult(("IN",), ((True,),))}
    )
    assert owner.missing_privileges(SCOPE, privileges) == ()


def test_locked_objects_are_those_held_in_the_schema() -> None:
    locks: tuple[dict[str, object], ...] = (
        {"resource": "DB.SCH.SALES", "status": "HOLDING"},
        {"resource": "DB.SCH.SALES", "status": "HOLDING"},
        {"resource": "DB.SCH.MENU", "status": "WAITING"},
        {"resource": "DB.OTHER.X", "status": "HOLDING"},
        {"resource": "not a name", "status": "HOLDING"},
    )
    connector = ScriptedConnector({"SHOW LOCKS IN ACCOUNT": locks})
    assert connector.locked_objects(SCOPE) == (QualifiedName.parse("DB.SCH.SALES"),)


def test_external_references_come_from_the_dependency_record_once_each() -> None:
    rows = (("BI", "DASH", "REVENUE"), ("BI", "DASH", "REVENUE"), ("bi", "Dash", "lower"))
    connector = ScriptedConnector(results={"SELECT REFERENCING_DATABASE": QueryResult(("D", "S", "N"), rows)})
    found = connector.external_references(QualifiedName.parse("DB.SCH.SALES"))
    assert [item.sql for item in found] == ["BI.DASH.REVENUE", '"bi"."Dash"."lower"']
    assert connector.sent[0][1] == ("DB", "SCH", "SALES")
