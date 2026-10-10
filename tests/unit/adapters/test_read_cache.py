"""The connector's read cache: repeats answered once, writes forget, and batched lookups by name.

A lookup by name while a cache is in use reads its schema's whole listing instead of SHOW ...
LIKE, and a table's existence from one INFORMATION_SCHEMA read per schema. Each test answers
both forms the way Snowflake does and checks the cached lookup keeps exactly the rows, and
gives exactly the answers, the lookup by name does.
"""

from __future__ import annotations

import re

import pytest
from snowflake.connector.errors import ProgrammingError

from snowflake_semantic_tools.adapters.snowflake.connector.session import ReadCache
from snowflake_semantic_tools.cli.wiring.plan import _keeping_reads, _reads_kept
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import sql
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession, Rows

# The agents SHOW lists in DB.S: names that need quoting, differ only in case, or hold LIKE's
# wildcards, so LIKE alone would list more than one of them.
_AGENTS = ("Mixed_Case", "MIXED_CASE", "MIXEDXCASE", "A%B", "AXYB", "PLAIN", "GRÖSSE", "Größe", "back\\slash")
_TABLES = {"ORDERS": "BASE TABLE", "Orders_View": "VIEW", "CUSTOMERS": "BASE TABLE"}
_SHOW_LIKE = re.compile(r"SHOW AGENTS LIKE '(?P<pattern>.*)' IN SCHEMA DB\.S$", re.DOTALL)


def _like(pattern: str, name: str) -> bool:
    """Whether Snowflake's LIKE, which ignores case, matches `name` against `pattern`."""
    expression = "".join(".*" if char == "%" else "." if char == "_" else re.escape(char) for char in pattern)
    return re.fullmatch(expression, name, re.IGNORECASE | re.DOTALL) is not None


def _snowflake(statement: str, binds: tuple[object, ...]) -> tuple[Rows | None, int]:
    """Answer the reads under test as Snowflake would."""
    rows: list[dict[str, object]] = [
        {"name": name, "database_name": "DB", "schema_name": "S", "comment": f"of {name}"} for name in _AGENTS
    ]
    if statement == "SHOW AGENTS IN SCHEMA DB.S":
        return rows, 0
    if (like := _SHOW_LIKE.fullmatch(statement)) is not None:
        pattern = like.group("pattern").replace("''", "'").replace("\\\\", "\\")
        return [row for row in rows if _like(pattern, str(row["name"]))], 0
    if statement.startswith("SHOW AGENTS"):
        return [], 0
    if statement.startswith("SELECT COUNT(*) FROM DB.INFORMATION_SCHEMA.TABLES"):
        schema, name = binds
        base_only = "BASE TABLE" in statement
        found = schema == "S" and name in _TABLES and (not base_only or _TABLES[str(name)] == "BASE TABLE")
        return [(1 if found else 0,)], 0
    if statement.startswith("SELECT TABLE_NAME, TABLE_TYPE FROM DB.INFORMATION_SCHEMA.TABLES"):
        return (list(_TABLES.items()) if binds == ("S",) else []), 0
    return None, 0


def _connector() -> tuple[FakeDriverConnector, FakeDriverSession]:
    session = FakeDriverSession(respond=_snowflake)
    return FakeDriverConnector(session), session


def _cached() -> tuple[FakeDriverConnector, FakeDriverSession]:
    connector, session = _connector()
    connector.keep_reads(ReadCache())
    return connector, session


_LOOKUPS = (
    'DB.S."Mixed_Case"',
    "DB.S.MIXED_CASE",
    'DB.S."A%B"',
    "DB.S.PLAIN",
    "DB.S.ABSENT",
    'DB.S."Größe"',
    'DB.S."back\\slash"',
)


@pytest.mark.parametrize("name", _LOOKUPS)
def test_a_cached_lookup_by_name_keeps_the_rows_show_like_keeps(name: str) -> None:
    target = QualifiedName.parse(name)
    plain, _ = _connector()
    cached, _ = _cached()
    assert cached.show_row("AGENT", target) == plain.show_row("AGENT", target)
    assert cached.object_exists("AGENT", target) == plain.object_exists("AGENT", target)
    assert cached.describe_marker(target, "AGENT") == plain.describe_marker(target, "AGENT")


def test_cached_lookups_by_ascii_name_read_the_schema_once() -> None:
    connector, session = _cached()
    for name in ('DB.S."Mixed_Case"', "DB.S.PLAIN", "DB.S.ABSENT", 'DB.S."A%B"'):
        connector.show_row("AGENT", QualifiedName.parse(name))
    assert session.statements == ["SHOW AGENTS IN SCHEMA DB.S"]


def test_a_cached_lookup_outside_ascii_or_with_a_backslash_still_asks_by_name() -> None:
    # LIKE may compare such a name otherwise than Python's case rules, or read its backslash
    # as an escape, so it is looked up by name, as without a cache; each once.
    connector, session = _cached()
    for _ in range(2):
        connector.show_row("AGENT", QualifiedName.parse('DB.S."Größe"'))
        connector.show_row("AGENT", QualifiedName.parse('DB.S."back\\slash"'))
    assert session.statements == [
        "SHOW AGENTS LIKE 'Größe' IN SCHEMA DB.S",
        "SHOW AGENTS LIKE 'back\\\\slash' IN SCHEMA DB.S",
    ]


def test_without_a_cache_each_lookup_asks_by_name() -> None:
    connector, session = _connector()
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert session.statements == ["SHOW AGENTS LIKE 'PLAIN' IN SCHEMA DB.S"] * 2


def test_a_lookup_in_a_schema_that_lists_nothing_finds_nothing() -> None:
    connector, session = _cached()
    assert connector.show_row("AGENT", QualifiedName.parse("DB.EMPTY.PLAIN")) is None
    assert not connector.object_exists("AGENT", QualifiedName.parse("DB.EMPTY.OTHER"))
    assert session.statements == ["SHOW AGENTS IN SCHEMA DB.EMPTY"]


@pytest.mark.parametrize("kind", ["TABLE", "TABLE OR VIEW"])
@pytest.mark.parametrize(
    "name", ["DB.S.ORDERS", 'DB.S."Orders_View"', "DB.S.ORDERS_VIEW", "DB.S.MISSING", "DB.T.ORDERS"]
)
def test_a_cached_table_existence_answers_as_the_count_by_name_does(kind: str, name: str) -> None:
    plain, _ = _connector()
    cached, _ = _cached()
    target = QualifiedName.parse(name)
    assert cached.object_exists(kind, target) == plain.object_exists(kind, target)


def test_cached_table_existence_reads_each_schema_once() -> None:
    connector, session = _cached()
    for name in ("DB.S.ORDERS", "DB.S.CUSTOMERS", "DB.S.MISSING", "DB.T.ORDERS"):
        connector.relation_exists(QualifiedName.parse(name))
        connector.object_exists("TABLE", QualifiedName.parse(name))
    assert [statement.split(" WHERE ")[0] for statement in session.statements] == [
        "SELECT TABLE_NAME, TABLE_TYPE FROM DB.INFORMATION_SCHEMA.TABLES"
    ] * 2
    assert [binds for _, binds in session.executed] == [("S",), ("T",)]


def test_a_repeated_show_describe_or_list_is_answered_once() -> None:
    connector, session = _cached()
    for _ in range(3):
        connector.observe_agent_versions(QualifiedName.parse("DB.S.PLAIN"))
        connector.show_grants("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert session.statements == ["SHOW VERSIONS IN AGENT DB.S.PLAIN", "SHOW GRANTS ON AGENT DB.S.PLAIN"]


@pytest.mark.parametrize(
    "write",
    [
        lambda connector: connector.execute_script((sql("CREATE SCHEMA DB.X"),)),
        lambda connector: connector.try_execute(sql("DROP AGENT DB.S.PLAIN")),
        lambda connector: connector.query(sql("ALTER AGENT DB.S.PLAIN SET COMMENT = 'x'")),
        lambda connector: connector._dict_rows(sql("CALL DB.S.WRITER()")),
    ],
    ids=["script", "statement", "query", "call"],
)
def test_a_write_forgets_every_answer(write: object) -> None:
    connector, session = _cached()
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    write(connector)  # type: ignore[operator]  # each case writes on the connector
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert session.statements.count("SHOW AGENTS IN SCHEMA DB.S") == 2


def test_a_transaction_forgets_every_answer() -> None:
    connector, session = _cached()
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    with connector._transaction_block() as transaction:
        transaction.run(sql("DELETE FROM DB.S.T"))
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert session.statements.count("SHOW AGENTS IN SCHEMA DB.S") == 2


def test_a_read_keeps_every_answer() -> None:
    connector, session = _cached()
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    connector.query(sql("SELECT CURRENT_ROLE()"))
    connector.query(sql("EXPLAIN SELECT 1"))
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert session.statements.count("SHOW AGENTS IN SCHEMA DB.S") == 1


def test_a_failed_read_is_read_again() -> None:
    failures = [ProgrammingError("SQL execution error: transient")]

    def flaky(statement: str, binds: tuple[object, ...]) -> tuple[Rows | None, int]:
        if failures:
            raise failures.pop()
        return _snowflake(statement, binds)

    session = FakeDriverSession(respond=flaky)
    connector = FakeDriverConnector(session)
    connector.keep_reads(ReadCache())
    with pytest.raises(SnowflakePortError, match="transient"):
        connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN")) is not None
    assert session.statements == ["SHOW AGENTS IN SCHEMA DB.S"] * 2


def test_reads_kept_for_planning_end_with_it_and_reach_its_siblings() -> None:
    connector, session = _connector()
    reads = ReadCache()
    with _reads_kept(connector, reads):
        connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
        connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    # Apply writes on this connection next, so nothing read while planning answers it.
    connector.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert session.statements == ["SHOW AGENTS IN SCHEMA DB.S", "SHOW AGENTS LIKE 'PLAIN' IN SCHEMA DB.S"]
    sibling, sibling_session = _connector()
    assert _keeping_reads(sibling, reads) is sibling
    sibling.show_row("AGENT", QualifiedName.parse("DB.S.PLAIN"))
    assert sibling_session.statements == []


def test_a_double_that_is_no_session_plans_without_a_cache() -> None:
    double = object()
    assert _keeping_reads(double, ReadCache()) is double
    with _reads_kept(double, ReadCache()):
        pass
