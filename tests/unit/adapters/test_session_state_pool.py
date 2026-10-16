"""The session's statement accounting, the state table's single write model, and the connector pool."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence

import pytest
from snowflake.connector.errors import OperationalError, ProgrammingError

from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool, SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockFence, StateWrite
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession, Rows

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
ENTRY = AppliedEntry("f" * 64, "DB.S.V", "2026-09-29T00:00:00Z", "run", "applied", "d" * 64, "m" * 64)
FENCE = LockFence("run", 3)


def _driver(
    failures: Mapping[str, BaseException] | None = None, rows: Mapping[str, Rows] | None = None
) -> FakeDriverSession:
    """A driver connection every statement of which reports two rows affected."""
    return FakeDriverSession(failures, rows, rowcount=2)


class DriverConnector(FakeDriverConnector):
    def __init__(self, driver: FakeDriverSession, *, exists: bool = True, columns: tuple[str, ...] = ()) -> None:
        super().__init__(driver)
        self._exists = exists
        self._columns = columns

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        return self._exists

    def _dict_rows(self, statement: object) -> tuple[dict[str, object], ...]:
        return tuple({"name": name} for name in self._columns)


def test_a_script_reports_exactly_the_statements_that_completed_before_its_failure() -> None:
    failure = ProgrammingError(msg="Object does not exist", errno=2003, sqlstate="02000")
    driver = _driver({"CREATE VIEW B": failure})
    result = DriverConnector(driver).execute_script(
        (sql("CREATE VIEW A AS SELECT 1"), sql("CREATE VIEW B AS SELECT 1"), sql("CREATE VIEW C AS SELECT 1"))
    )
    assert not result.ok
    assert result.query_ids == ("q1",)
    assert result.rows_affected == 2
    assert result.error is not None and (result.error.sqlstate, result.error.errno) == ("02000", 2003)
    assert driver.statements == ["CREATE VIEW A AS SELECT 1", "CREATE VIEW B AS SELECT 1"]


def test_a_script_that_completes_reports_every_statement_and_its_rows() -> None:
    result = DriverConnector(_driver()).execute_script((sql("SELECT 1"), sql("SELECT 2")))
    assert (result.ok, result.query_ids, result.rows_affected) == (True, ("q1", "q2"), 4)


def test_a_script_whose_cursor_cannot_open_ran_nothing() -> None:
    result = DriverConnector(_driver({"cursor": OperationalError(msg="Connection is closed")})).execute_script(
        (sql("SELECT 1"),)
    )
    assert (result.ok, result.query_ids, result.rows_affected) == (False, (), 0)


def test_a_programming_error_inside_a_script_is_never_reported_as_a_snowflake_failure() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        DriverConnector(_driver({"SELECT": TypeError("unsupported parameter type: Decimal")})).execute_script(
            (sql("SELECT 1"),)
        )


def test_a_state_write_merges_the_changes_in_one_fenced_transaction_with_every_value_bound() -> None:
    driver = _driver(rows={"SELECT RUN_ID, GENERATION": [("run", 3)]})
    hostile = "semantic_view:v'; DROP TABLE x; --"
    assert DriverConnector(driver, exists=False).write_state(
        STATE_TABLE, "dev", "m" * 64, StateWrite({"skill:b": ENTRY, hostile: ENTRY}, ("agent:gone",)), FENCE
    )
    statements = driver.statements
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS DB.S.SST_STATE (")
    begin = statements.index("BEGIN")
    assert [statement.split(" ")[0] for statement in statements[begin:]] == [
        "BEGIN",
        "UPDATE",
        "SELECT",
        "MERGE",
        "DELETE",
        "UPDATE",
        "COMMIT",
    ]
    # The mutex row first, then the fence, both on the lock table, before any state row.
    assert driver.executed[begin + 1] == (
        "UPDATE DB.S.SST_STATE_LOCK SET ACQUIRED_AT = CURRENT_TIMESTAMP() WHERE TARGET_NAME = %s",
        ("",),
    )
    assert driver.executed[begin + 2] == (
        "SELECT RUN_ID, GENERATION FROM DB.S.SST_STATE_LOCK WHERE TARGET_NAME = %s",
        ("dev",),
    )
    assert all("DROP TABLE x" not in statement for statement in statements)
    merged = [(statement, params) for statement, params in driver.executed if statement.startswith("MERGE")]
    # One SELECT of bound values per entry, in key order, every entry's thirteen values bound.
    assert [statement.count("SELECT ") for statement, _ in merged] == [2]
    params = merged[0][1]
    assert params is not None and len(params) == 26
    assert [params[1], params[14]] == ["semantic_view:v'; DROP TABLE x; --", "skill:b"]
    assert driver.executed[-3] == (
        "DELETE FROM DB.S.SST_STATE WHERE TARGET_NAME = %s AND ARTIFACT_KEY = %s",
        ("dev", "agent:gone"),
    )
    assert driver.executed[-2] == (
        "UPDATE DB.S.SST_STATE SET STATE_MANIFEST_ID = %s WHERE TARGET_NAME = %s",
        ("m" * 64, "dev"),
    )


def test_a_state_write_of_many_entries_merges_them_in_batches_in_key_order() -> None:
    driver = _driver(rows={"SELECT RUN_ID, GENERATION": [("run", 3)]})
    upserts = {f"skill:s{index:03d}": ENTRY for index in range(120)}
    assert DriverConnector(driver).write_state(STATE_TABLE, "dev", "m" * 64, StateWrite(upserts, ()), FENCE)
    merged = [(statement, params) for statement, params in driver.executed if statement.startswith("MERGE")]
    assert [statement.count("SELECT ") for statement, _ in merged] == [50, 50, 20]
    keys = [params[index] for _, params in merged if params is not None for index in range(1, len(params), 13)]
    assert keys == sorted(upserts)


def test_a_state_write_whose_fence_no_longer_holds_rolls_back_having_written_nothing() -> None:
    driver = _driver(rows={"SELECT RUN_ID, GENERATION": [("run", 4)]})
    assert not DriverConnector(driver).write_state(STATE_TABLE, "dev", "m", StateWrite({"k": ENTRY}), FENCE)
    assert driver.statements[-1] == "ROLLBACK"
    assert not any(statement.startswith(("MERGE", "DELETE")) for statement in driver.statements)


def test_a_state_write_that_fails_part_way_rolls_back_and_records_nothing() -> None:
    driver = _driver(
        {"DELETE FROM": ProgrammingError(msg="lock timeout", errno=625, sqlstate="57014")},
        rows={"SELECT RUN_ID, GENERATION": [("run", 3)]},
    )
    with pytest.raises(SnowflakePortError, match="lock timeout"):
        DriverConnector(driver).write_state(STATE_TABLE, "dev", "m", StateWrite({"k": ENTRY}, ("old",)), FENCE)
    assert driver.statements[-1] == "ROLLBACK"
    assert "COMMIT" not in driver.statements


def test_ensure_state_table_creates_an_absent_table_with_every_column() -> None:
    driver = _driver()
    DriverConnector(driver, exists=False).ensure_state_table(STATE_TABLE)
    [create] = driver.statements
    assert "STATE_MANIFEST_ID VARCHAR(64)" in create and ", PRIMARY KEY (TARGET_NAME, ARTIFACT_KEY))" in create


def test_ensure_state_table_adds_only_the_columns_an_older_table_lacks() -> None:
    driver = _driver()
    DriverConnector(driver, columns=("TARGET_NAME", "PHYSICAL_RESOURCES")).ensure_state_table(STATE_TABLE)
    assert driver.statements == [
        "ALTER TABLE DB.S.SST_STATE ADD COLUMN IF NOT EXISTS COMPONENT_FINGERPRINTS OBJECT",
        "ALTER TABLE DB.S.SST_STATE ADD COLUMN IF NOT EXISTS LIVE_DIGEST VARCHAR",
        "ALTER TABLE DB.S.SST_STATE ADD COLUMN IF NOT EXISTS STATE_MANIFEST_ID VARCHAR(64)",
    ]


def test_an_existing_state_table_gets_no_ddl_so_a_role_that_may_only_write_it_can() -> None:
    # Snowflake checks CREATE TABLE on the schema for CREATE TABLE IF NOT EXISTS even when the
    # table exists, which a role granted only SELECT, INSERT, UPDATE and DELETE lacks.
    driver = _driver(rows={"SELECT RUN_ID, GENERATION": [("run", 3)]})
    columns = ("COMPONENT_FINGERPRINTS", "PHYSICAL_RESOURCES", "LIVE_DIGEST", "STATE_MANIFEST_ID")
    assert DriverConnector(driver, columns=columns).write_state(
        STATE_TABLE, "dev", "m", StateWrite({"k": ENTRY}), FENCE
    )
    assert not any(statement.startswith(("CREATE", "ALTER")) for statement in driver.statements)


@pytest.mark.parametrize(
    ("exists", "columns", "recorded", "expected"),
    [
        (False, (), [], None),
        (True, ("TARGET_NAME",), [], None),
        (True, ("STATE_MANIFEST_ID",), [("m1",), (None,)], "m1"),
        (True, ("STATE_MANIFEST_ID",), [("m1",), ("m2",)], None),
        (True, ("STATE_MANIFEST_ID",), [], None),
    ],
    ids=["no-table", "unmigrated", "one-manifest", "two-manifests", "no-rows"],
)
def test_the_state_manifest_is_read_back_only_when_one_is_recorded(
    exists: bool, columns: tuple[str, ...], recorded: list[tuple[object, ...]], expected: str | None
) -> None:
    driver = _driver(rows={"SELECT DISTINCT STATE_MANIFEST_ID": recorded})
    connector = DriverConnector(driver, exists=exists, columns=columns)
    assert connector.read_state_manifest(STATE_TABLE, "dev") == expected
    assert all(not statement.startswith(("CREATE", "ALTER")) for statement in driver.statements)


def test_a_sibling_connects_with_the_same_settings_and_shares_no_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    opened: list[dict[str, object]] = []

    def connect(**params: object) -> FakeDriverSession:
        opened.append(params)
        return _driver()

    monkeypatch.setattr("snowflake.connector.connect", connect)
    first = SnowflakeConnector({"account": "acme", "role": "R"})
    second = first.sibling()
    assert isinstance(second, SnowflakeConnector) and second is not first
    assert opened == [{"account": "acme", "role": "R"}, {"account": "acme", "role": "R"}]
    assert second._connection is not first._connection


def _connecting(
    monkeypatch: pytest.MonkeyPatch, current: tuple[object, object] = ("DB", "S")
) -> list[tuple[dict[str, object], FakeDriverSession]]:
    """Make every connect open a recorded driver whose session starts in `current`."""
    opened: list[tuple[dict[str, object], FakeDriverSession]] = []

    def connect(**params: object) -> FakeDriverSession:
        driver = _driver(rows={"SELECT CURRENT_DATABASE": [current], "SELECT 1": [(1,)]})
        opened.append((params, driver))
        return driver

    monkeypatch.setattr("snowflake.connector.connect", connect)
    return opened


def test_query_in_context_connects_a_session_in_its_scope_and_never_runs_a_use(monkeypatch: pytest.MonkeyPatch) -> None:
    opened = _connecting(monkeypatch)
    connector = SnowflakeConnector({"account": "acme", "database": "OTHER"})
    scope = SchemaScope.from_qualified_name(STATE_TABLE)
    assert connector.query_in_context(scope, sql("SELECT 1")).rows == ((1,),)
    connector.query_in_context(scope, sql("SELECT 1"))
    (_, main), (settings, scoped) = opened
    assert settings == {"account": "acme", "database": "DB", "schema": "S"}
    assert main.statements == []
    assert scoped.statements == ["SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()", "SELECT 1", "SELECT 1"]
    assert not any(statement.startswith("USE") for _, driver in opened for statement in driver.statements)
    connector.close()
    assert main.closed and scoped.closed


def test_each_scope_has_a_session_of_its_own_and_a_quoted_name_keeps_its_quotes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = _connecting(monkeypatch, ("my db", "S"))
    connector = SnowflakeConnector({"account": "acme"})
    quoted = SchemaScope(Identifier("my db", quoted=True), Identifier("S"))
    connector.query_in_context(quoted, sql("SELECT 1"))
    assert opened[1][0] == {"account": "acme", "database": '"my db"', "schema": "S"}
    with pytest.raises(SnowflakePortError, match="started in my db.S"):
        connector.query_in_context(SchemaScope.from_qualified_name(STATE_TABLE), sql("SELECT 1"))
    # The session that started elsewhere was closed, never kept for its scope.
    assert opened[2][1].closed and len(opened) == 3
    connector.close()


def test_a_halted_session_starts_no_statement_and_halts_its_scoped_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    opened = _connecting(monkeypatch)
    connector = SnowflakeConnector({"account": "acme"})
    scope = SchemaScope.from_qualified_name(STATE_TABLE)
    connector.query_in_context(scope, sql("SELECT 1"))
    connector.halt("the run lock was lost")
    connector.halt("a later reason")
    with pytest.raises(SnowflakePortError, match="the run lock was lost"):
        connector.query(sql("SELECT 1"))
    with pytest.raises(SnowflakePortError, match="the run lock was lost"):
        connector.query_in_context(scope, sql("SELECT 1"))
    result = connector.execute_script((sql("SELECT 1"),))
    assert (result.ok, result.query_ids) == (False, ())
    assert result.error is not None and result.error.message == "the run lock was lost"
    assert opened[0][1].statements == [] and opened[1][1].statements == [
        "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()",
        "SELECT 1",
    ]


def test_a_script_halted_part_way_stops_before_its_next_statement() -> None:
    driver = _driver()
    connector = DriverConnector(driver)
    halting = driver.execute

    def execute(
        statement: str,
        params: Sequence[object] | None = None,
        *,
        num_statements: int | None = None,
        timeout: int | None = None,
    ) -> None:
        halting(statement, params, num_statements=num_statements, timeout=timeout)
        connector.halt("lost")

    driver.execute = execute  # type: ignore[method-assign]
    result = connector.execute_script((sql("SELECT 1"), sql("SELECT 2")))
    assert (result.ok, result.query_ids) == (False, ("q1",))
    assert result.error is not None and result.error.message == "lost"
    assert driver.statements == ["SELECT 1"]


def test_a_guard_runs_before_each_write_statement_of_a_script_and_never_before_a_read() -> None:
    driver = _driver()
    connector = DriverConnector(driver)
    seen: list[int] = []
    connector.guard_writes(lambda: seen.append(len(driver.statements)))
    script = (sql("SELECT 1"), sql("ALTER AGENT DB.S.A SET COMMENT = 'a'"), sql("ALTER AGENT DB.S.A SET COMMENT = 'b'"))
    assert connector.execute_script(script).ok
    # Before the second statement and the third: after one, then two, statements ran.
    assert seen == [1, 2]
    connector.guard_writes(None)
    assert connector.execute_script(script).ok and seen == [1, 2]


def test_a_guard_that_halts_the_session_stops_the_script_before_its_statement() -> None:
    driver = _driver()
    connector = DriverConnector(driver)
    guarded = iter((None, "the run lock was lost"))

    def guard() -> None:
        reason = next(guarded)
        if reason is not None:
            connector.halt(reason)

    connector.guard_writes(guard)
    result = connector.execute_script((sql("ALTER VIEW A SET COMMENT = 'a'"), sql("ALTER VIEW B SET COMMENT = 'b'")))
    assert (result.ok, result.query_ids) == (False, ("q1",))
    assert result.error is not None and result.error.message == "the run lock was lost"
    assert driver.statements == ["ALTER VIEW A SET COMMENT = 'a'"]


def test_a_guard_that_cannot_read_what_it_checks_fails_the_script_with_that_error() -> None:
    driver = _driver()
    connector = DriverConnector(driver)

    def guard() -> None:
        raise SnowflakePortError("Session no longer exists", errno=390111, sqlstate="08001")

    connector.guard_writes(guard)
    result = connector.execute_script((sql("ALTER VIEW A SET COMMENT = 'a'"),))
    assert (result.ok, result.query_ids, driver.statements) == (False, (), [])
    assert result.error is not None and (result.error.errno, result.error.sqlstate) == (390111, "08001")


def test_a_scoped_session_opened_after_a_halt_is_halted_too(monkeypatch: pytest.MonkeyPatch) -> None:
    _connecting(monkeypatch)
    connector = SnowflakeConnector({"account": "acme"})
    connector.halt("lost")
    with pytest.raises(SnowflakePortError, match="lost"):
        connector.query_in_context(SchemaScope.from_qualified_name(STATE_TABLE), sql("SELECT 1"))


class _Pooled:
    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False
        self.halted: str | None = None

    def halt(self, reason: str) -> None:
        self.halted = reason

    def close(self) -> None:
        self.closed = True
        if self.name == "broken":
            raise OSError("close failed")


def test_a_pool_opens_siblings_only_as_leases_overlap_and_never_lends_anything_else() -> None:
    opened: list[_Pooled] = []

    def open_sibling() -> _Pooled:
        opened.append(_Pooled(f"s{len(opened)}"))
        return opened[-1]

    with ConnectorPool(3, open_sibling) as pool:
        with pool.lease() as one:
            assert one is opened[0]
        with pool.lease() as again:
            assert again is one and len(opened) == 1
        with pool.lease() as a, pool.lease() as b, pool.lease() as c:
            assert {id(a), id(b), id(c)} == {id(item) for item in opened} and pool.opened == 3
            with pytest.raises(RuntimeError, match="exhausted"), pool.lease():
                pass
    assert [item.closed for item in opened] == [True, True, True] and pool.opened == 0


def test_a_halted_pool_halts_every_sibling_it_opened_and_every_one_it_opens_later() -> None:
    siblings = iter((_Pooled("a"), _Pooled("b")))
    with ConnectorPool(2, lambda: next(siblings)) as pool:
        with pool.lease() as early:
            pass
        pool.halt("lost")
        pool.halt("later")
        with pool.lease() as reused, pool.lease() as late:
            assert reused is early
        assert (early.halted, late.halted) == ("lost", "lost")


def test_a_pool_closes_every_sibling_even_when_one_fails_to_close() -> None:
    siblings = iter((_Pooled("broken"), _Pooled("fine")))
    pool = ConnectorPool(3, lambda: next(siblings))
    with pool.lease() as broken, pool.lease() as fine:
        pass
    with pytest.raises(OSError, match="close failed"):
        pool.close()
    assert broken.closed and fine.closed and pool.opened == 0


def test_a_sibling_that_cannot_open_frees_its_slot() -> None:
    attempts: list[int] = []

    def open_sibling() -> _Pooled:
        attempts.append(1)
        if len(attempts) == 1:
            raise SnowflakePortError("login failed")
        return _Pooled("late")

    pool = ConnectorPool(1, open_sibling)
    with pytest.raises(SnowflakePortError), pool.lease():
        pass
    with pool.lease() as late:
        assert late.name == "late"


def test_a_pool_needs_room_for_one_connector() -> None:
    with pytest.raises(ValueError, match="at least one"):
        ConnectorPool(0, lambda: _Pooled("x"))


def test_parallel_leases_never_share_a_connector() -> None:
    pool = ConnectorPool(4, lambda: _Pooled("sibling"))
    in_use: set[int] = set()
    clash: list[bool] = []
    guard = threading.Lock()
    start = threading.Barrier(4)

    def work() -> None:
        start.wait(timeout=5)
        for _ in range(50):
            with pool.lease() as connector:
                with guard:
                    clash.append(id(connector) in in_use)
                    in_use.add(id(connector))
                with guard:
                    in_use.discard(id(connector))

    threads = [threading.Thread(target=work) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert len(clash) == 200 and not any(clash)
    pool.close()
