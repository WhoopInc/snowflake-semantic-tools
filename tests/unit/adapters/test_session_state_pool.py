"""The session's statement accounting, the state table's single write model, and the connector pool."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from threading import RLock

import pytest
from snowflake.connector.errors import OperationalError, ProgrammingError

from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool, SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import StateWrite

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
ENTRY = AppliedEntry("f" * 64, "DB.S.V", "2026-09-29T00:00:00Z", "run", "applied", "d" * 64, "m" * 64)


class _Driver:
    """A driver connection double: one cursor, every statement and its binds recorded, failures by prefix."""

    def __init__(
        self,
        failures: Mapping[str, BaseException] | None = None,
        rows: Mapping[str, list[tuple[object, ...]]] | None = None,
    ) -> None:
        self.failures = dict(failures or {})
        self.rows = dict(rows or {})
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.description: tuple[tuple[str], ...] | None = None
        self.sfqid = ""
        self.rowcount = 0
        self.closed = False
        self._result: list[tuple[object, ...]] = []

    def cursor(self, *args: object) -> _Driver:
        if "cursor" in self.failures:
            raise self.failures["cursor"]
        return self

    def execute(self, statement: str, params: Sequence[object] | None = None) -> None:
        self.executed.append((statement, tuple(params or ())))
        failure = next((error for prefix, error in self.failures.items() if statement.startswith(prefix)), None)
        if failure is not None:
            raise failure
        self.sfqid = f"q{len(self.executed)}"
        self.rowcount = 2
        found = next((rows for prefix, rows in self.rows.items() if statement.startswith(prefix)), None)
        self._result = list(found or [])
        self.description = (("C",),) if found is not None else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._result

    def close(self) -> None:
        self.closed = True

    @property
    def statements(self) -> list[str]:
        return [statement for statement, _ in self.executed]


class DriverConnector(SnowflakeConnector):
    def __init__(self, driver: _Driver, *, exists: bool = True, columns: tuple[str, ...] = ()) -> None:
        self._lock = RLock()
        self._connection = driver  # type: ignore[assignment]  # a double, not a driver connection
        self._exists = exists
        self._columns = columns

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        return self._exists

    def _dict_rows(self, statement: object) -> tuple[dict[str, object], ...]:
        return tuple({"name": name} for name in self._columns)


def test_a_script_reports_exactly_the_statements_that_completed_before_its_failure() -> None:
    failure = ProgrammingError(msg="Object does not exist", errno=2003, sqlstate="02000")
    driver = _Driver({"CREATE VIEW B": failure})
    result = DriverConnector(driver).execute_script(
        (sql("CREATE VIEW A AS SELECT 1"), sql("CREATE VIEW B AS SELECT 1"), sql("CREATE VIEW C AS SELECT 1"))
    )
    assert not result.ok
    assert result.query_ids == ("q1",)
    assert result.rows_affected == 2
    assert result.error is not None and (result.error.sqlstate, result.error.errno) == ("02000", 2003)
    assert driver.statements == ["CREATE VIEW A AS SELECT 1", "CREATE VIEW B AS SELECT 1"]


def test_a_script_that_completes_reports_every_statement_and_its_rows() -> None:
    result = DriverConnector(_Driver()).execute_script((sql("SELECT 1"), sql("SELECT 2")))
    assert (result.ok, result.query_ids, result.rows_affected) == (True, ("q1", "q2"), 4)


def test_a_script_whose_cursor_cannot_open_ran_nothing() -> None:
    result = DriverConnector(_Driver({"cursor": OperationalError(msg="Connection is closed")})).execute_script(
        (sql("SELECT 1"),)
    )
    assert (result.ok, result.query_ids, result.rows_affected) == (False, (), 0)


def test_a_programming_error_inside_a_script_is_never_reported_as_a_snowflake_failure() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        DriverConnector(_Driver({"SELECT": TypeError("unsupported parameter type: Decimal")})).execute_script(
            (sql("SELECT 1"),)
        )


def test_a_state_write_merges_each_change_in_one_transaction_with_every_value_bound() -> None:
    driver = _Driver()
    hostile = "semantic_view:v'; DROP TABLE x; --"
    DriverConnector(driver).write_state(
        STATE_TABLE, "dev", "m" * 64, StateWrite({"skill:b": ENTRY, hostile: ENTRY}, ("agent:gone",))
    )
    statements = driver.statements
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS DB.S.SST_STATE (")
    begin = statements.index("BEGIN")
    assert [statement.split(" ")[0] for statement in statements[begin:]] == [
        "BEGIN",
        "MERGE",
        "MERGE",
        "DELETE",
        "UPDATE",
        "COMMIT",
    ]
    assert all("DROP TABLE x" not in statement for statement in statements)
    merged_keys = [params[1] for statement, params in driver.executed if statement.startswith("MERGE")]
    assert merged_keys == ["semantic_view:v'; DROP TABLE x; --", "skill:b"]
    assert driver.executed[-3] == (
        "DELETE FROM DB.S.SST_STATE WHERE TARGET_NAME = %s AND ARTIFACT_KEY = %s",
        ("dev", "agent:gone"),
    )
    assert driver.executed[-2] == (
        "UPDATE DB.S.SST_STATE SET STATE_MANIFEST_ID = %s WHERE TARGET_NAME = %s",
        ("m" * 64, "dev"),
    )


def test_a_state_write_that_fails_part_way_rolls_back_and_records_nothing() -> None:
    driver = _Driver({"DELETE FROM": ProgrammingError(msg="lock timeout", errno=625, sqlstate="57014")})
    with pytest.raises(SnowflakePortError, match="lock timeout"):
        DriverConnector(driver).write_state(STATE_TABLE, "dev", "m", StateWrite({"k": ENTRY}, ("old",)))
    assert driver.statements[-1] == "ROLLBACK"
    assert "COMMIT" not in driver.statements


def test_ensure_state_table_migrates_an_older_table_idempotently() -> None:
    driver = _Driver()
    DriverConnector(driver).ensure_state_table(STATE_TABLE)
    create, *alters = driver.statements
    assert "STATE_MANIFEST_ID VARCHAR(64)" in create and ", PRIMARY KEY (TARGET_NAME, ARTIFACT_KEY))" in create
    assert alters == [
        "ALTER TABLE DB.S.SST_STATE ADD COLUMN IF NOT EXISTS COMPONENT_FINGERPRINTS OBJECT",
        "ALTER TABLE DB.S.SST_STATE ADD COLUMN IF NOT EXISTS PHYSICAL_RESOURCES ARRAY",
        "ALTER TABLE DB.S.SST_STATE ADD COLUMN IF NOT EXISTS STATE_MANIFEST_ID VARCHAR(64)",
    ]


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
    driver = _Driver(rows={"SELECT DISTINCT STATE_MANIFEST_ID": recorded})
    connector = DriverConnector(driver, exists=exists, columns=columns)
    assert connector.read_state_manifest(STATE_TABLE, "dev") == expected
    assert all(not statement.startswith(("CREATE", "ALTER")) for statement in driver.statements)


def test_a_sibling_connects_with_the_same_settings_and_shares_no_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    opened: list[dict[str, object]] = []

    def connect(**params: object) -> _Driver:
        opened.append(params)
        return _Driver()

    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.session.snowflake.connector.connect", connect
    )
    first = SnowflakeConnector({"account": "acme", "role": "R"})
    second = first.sibling()
    assert isinstance(second, SnowflakeConnector) and second is not first
    assert opened == [{"account": "acme", "role": "R"}, {"account": "acme", "role": "R"}]
    assert second._connection is not first._connection


def test_closing_a_session_closes_the_scoped_session_it_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    drivers: list[_Driver] = []

    def connect(**params: object) -> _Driver:
        drivers.append(_Driver(rows={"SELECT": [(1,)]}))
        return drivers[-1]

    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.session.snowflake.connector.connect", connect
    )
    connector = SnowflakeConnector({"account": "acme"})
    scope = SchemaScope.from_qualified_name(STATE_TABLE)
    assert connector.query_in_context(scope, sql("SELECT 1")).rows == ((1,),)
    connector.query_in_context(scope, sql("SELECT 1"))
    main, scoped = drivers
    assert main.statements == [] and scoped.statements[:2] == ["USE DATABASE DB", "USE SCHEMA DB.S"]
    connector.close()
    assert main.closed and scoped.closed and len(drivers) == 2


class _Pooled:
    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True
        if self.name == "broken":
            raise OSError("close failed")


def test_a_pool_lends_the_first_connector_then_opens_siblings_only_as_leases_overlap() -> None:
    first = _Pooled("first")
    opened: list[_Pooled] = []

    def open_sibling() -> _Pooled:
        opened.append(_Pooled(f"s{len(opened)}"))
        return opened[-1]

    with ConnectorPool(first, 3, open_sibling) as pool:
        with pool.lease() as one:
            assert one is first
        with pool.lease() as again:
            assert again is first and opened == []
        with pool.lease() as a, pool.lease() as b, pool.lease() as c:
            assert len({id(a), id(b), id(c)}) == 3 and pool.opened == 2
            with pytest.raises(RuntimeError, match="exhausted"), pool.lease():
                pass
    assert [item.closed for item in opened] == [True, True] and not first.closed


def test_a_pool_closes_every_sibling_even_when_one_fails_to_close() -> None:
    siblings = iter((_Pooled("broken"), _Pooled("fine")))
    pool = ConnectorPool(_Pooled("first"), 3, lambda: next(siblings))
    with pool.lease(), pool.lease() as broken, pool.lease() as fine:
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

    pool = ConnectorPool(_Pooled("first"), 2, open_sibling)
    with pool.lease():
        with pytest.raises(SnowflakePortError), pool.lease():
            pass
        with pool.lease() as late:
            assert late.name == "late"


def test_a_pool_needs_room_for_one_connector() -> None:
    with pytest.raises(ValueError, match="at least one"):
        ConnectorPool(_Pooled("first"), 0, lambda: _Pooled("x"))


def test_parallel_leases_never_share_a_connector() -> None:
    pool = ConnectorPool(_Pooled("first"), 4, lambda: _Pooled("sibling"))
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
