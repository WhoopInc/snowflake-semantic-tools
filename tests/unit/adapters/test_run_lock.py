"""The run lock against a driver double that implements the lock table's MERGE semantics.

`LockDatabase` is the lock table: it answers the connector's statements by their binds, runs
each MERGE as one atomic compare-and-set (Snowflake serialises MERGEs on a table), and judges
expiry by its own clock, as Snowflake's CURRENT_TIMESTAMP does.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import RLock

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state.lock import LockClaim

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
_EPOCH = datetime(2026, 10, 1, tzinfo=UTC)


@dataclass
class _Row:
    target: str
    run_id: str
    owner: str
    host: str
    acquired_at: float
    expires_at: float


class LockDatabase:
    def __init__(self) -> None:
        self.now = 0.0
        self.rows: list[_Row] = []
        self.statements: list[str] = []
        self.guard = threading.Lock()
        # Run just before and just after a connector reads the lock, to stage a race.
        self.before_read: Callable[[], object] = lambda: None
        self.after_read: Callable[[], object] = lambda: None

    def execute(self, statement: str, params: Sequence[object]) -> tuple[list[tuple[object, ...]] | None, int]:
        with self.guard:
            self.statements.append(statement)
        if statement.startswith("CREATE TABLE IF NOT EXISTS DB.S.SST_STATE_LOCK"):
            return None, 0
        if statement.startswith("SELECT RUN_ID, OWNER, HOST"):
            self.before_read()
            with self.guard:
                held = sorted(
                    (row for row in self.rows if row.target == params[0]), key=lambda row: (row.acquired_at, row.run_id)
                )
                found = [self._shown(row) for row in held]
            self.after_read()
            return found, len(found)
        if statement.startswith("MERGE INTO DB.S.SST_STATE_LOCK AS held"):
            return None, self._claim(*params)
        if statement.startswith("UPDATE DB.S.SST_STATE_LOCK SET EXPIRES_AT"):
            ttl, target, run_id = params
            with self.guard:
                held = [row for row in self.rows if row.target == target and row.run_id == run_id]
                for row in held:
                    row.expires_at = self.now + float(str(ttl))
            return None, len(held)
        if statement.startswith("DELETE FROM DB.S.SST_STATE_LOCK"):
            target, run_id = params
            with self.guard:
                before = len(self.rows)
                self.rows = [row for row in self.rows if not (row.target == target and row.run_id == run_id)]
                return None, before - len(self.rows)
        raise AssertionError(f"unexpected statement: {statement}")

    def _claim(self, *params: object) -> int:
        target, run_id, owner, host, ttl, expected = params
        with self.guard:
            matched = [row for row in self.rows if row.target == target]
            if not matched:
                self.rows.append(
                    _Row(str(target), str(run_id), str(owner), str(host), self.now, self.now + float(str(ttl)))
                )
                return 1
            changed = 0
            for row in matched:
                if row.run_id == expected and row.expires_at <= self.now:
                    row.run_id, row.owner, row.host = str(run_id), str(owner), str(host)
                    row.acquired_at, row.expires_at = self.now, self.now + float(str(ttl))
                    changed += 1
            return changed

    def _shown(self, row: _Row) -> tuple[object, ...]:
        return (
            row.run_id,
            row.owner,
            row.host,
            _EPOCH + timedelta(seconds=row.acquired_at),
            _EPOCH + timedelta(seconds=row.expires_at),
            row.expires_at <= self.now,
        )


class _Cursor:
    def __init__(self, database: LockDatabase) -> None:
        self._database = database
        self.description: tuple[tuple[str], ...] | None = None
        self.rowcount = 0
        self.sfqid = "query-id"
        self._rows: list[tuple[object, ...]] = []

    def execute(self, statement: str, params: Sequence[object] | None = None) -> None:
        rows, self.rowcount = self._database.execute(statement, tuple(params or ()))
        self._rows = rows or []
        self.description = (("RUN_ID",),) if rows is not None else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, database: LockDatabase) -> None:
        self._database = database

    def cursor(self, *args: object) -> _Cursor:
        return _Cursor(self._database)


class LockConnector(SnowflakeConnector):
    def __init__(self, database: LockDatabase) -> None:
        self._lock = RLock()
        self._connection = _Connection(database)  # type: ignore[assignment]  # a double, not a driver connection


def test_a_free_lock_is_claimed_and_records_the_claim() -> None:
    database = LockDatabase()
    acquisition = LockConnector(database).acquire_run_lock(
        STATE_TABLE, "dev", LockClaim("run-a", "ROLE", "laptop", 60), break_stale=False
    )
    assert (acquisition.acquired, acquisition.holder, acquisition.broke_stale) == (True, None, False)
    assert [(row.run_id, row.owner, row.host, row.expires_at) for row in database.rows] == [
        ("run-a", "ROLE", "laptop", 60.0)
    ]


def test_a_live_lock_is_refused_and_names_its_holder_even_with_break_stale() -> None:
    database = LockDatabase()
    LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim("run-a", "ROLE", "ci"), break_stale=False)
    refused = LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim("run-b"), break_stale=True)
    assert not refused.acquired and refused.holder is not None
    assert refused.holder.describe() == "run run-a (ROLE on ci), expires 2026-10-01T00:30:00Z"
    assert [row.run_id for row in database.rows] == ["run-a"]
    assert not any(statement.startswith("MERGE") for statement in database.statements[-1:])


def test_an_expired_lock_is_taken_over_only_with_break_stale() -> None:
    database = LockDatabase()
    LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim("run-a", ttl_seconds=10), break_stale=False)
    database.now = 11.0
    kept = LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim("run-b"), break_stale=False)
    assert not kept.acquired and kept.holder is not None and kept.holder.expired
    broke = LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim("run-b"), break_stale=True)
    assert broke.acquired and broke.broke_stale and broke.holder is not None
    assert broke.holder.run_id == "run-a"
    assert [row.run_id for row in database.rows] == ["run-b"]


def test_two_runs_racing_for_a_free_lock_leave_exactly_one_holder() -> None:
    database = LockDatabase()
    both_read = threading.Barrier(2)
    database.after_read = lambda: both_read.wait(timeout=5) if len(database.statements) <= 4 else None
    results: dict[str, bool] = {}

    def claim(run_id: str) -> None:
        acquisition = LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim(run_id), break_stale=False)
        results[run_id] = acquisition.acquired

    threads = [threading.Thread(target=claim, args=(run_id,)) for run_id in ("run-a", "run-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert sorted(results.values()) == [False, True]
    winner = next(run_id for run_id, acquired in results.items() if acquired)
    assert [row.run_id for row in database.rows] == [winner]


def test_two_runs_racing_to_break_one_expired_lock_leave_exactly_one_holder() -> None:
    database = LockDatabase()
    LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim("old", ttl_seconds=1), break_stale=False)
    database.now = 5.0
    both_read = threading.Barrier(2)
    database.after_read = lambda: both_read.wait(timeout=5) if len(database.statements) <= 8 else None
    results: dict[str, bool] = {}

    def claim(run_id: str) -> None:
        results[run_id] = (
            LockConnector(database).acquire_run_lock(STATE_TABLE, "dev", LockClaim(run_id), break_stale=True).acquired
        )

    threads = [threading.Thread(target=claim, args=(run_id,)) for run_id in ("run-a", "run-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert sorted(results.values()) == [False, True]
    assert len(database.rows) == 1 and database.rows[0].run_id in ("run-a", "run-b")


def test_a_claim_that_lands_beside_a_rival_row_withdraws() -> None:
    # A standard table does not enforce the key: if a rival's insert landed too, neither holds the lock.
    database = LockDatabase()
    connector = LockConnector(database)

    def rival_inserts() -> None:
        if any(row.run_id == "mine" for row in database.rows) and len(database.rows) == 1:
            database.rows.append(_Row("dev", "rival", "", "", 0.0, 60.0))

    database.before_read = rival_inserts
    acquisition = connector.acquire_run_lock(STATE_TABLE, "dev", LockClaim("mine"), break_stale=False)
    assert not acquisition.acquired and acquisition.holder is not None
    assert acquisition.holder.run_id == "rival"
    assert [row.run_id for row in database.rows] == ["rival"]


def test_extend_and_release_touch_only_the_holders_row() -> None:
    database = LockDatabase()
    connector = LockConnector(database)
    claim = LockClaim("run-a", ttl_seconds=10)
    connector.acquire_run_lock(STATE_TABLE, "dev", claim, break_stale=False)
    database.now = 5.0
    assert connector.extend_run_lock(STATE_TABLE, "dev", claim)
    assert database.rows[0].expires_at == 15.0
    assert not connector.extend_run_lock(STATE_TABLE, "dev", LockClaim("run-b"))
    connector.release_run_lock(STATE_TABLE, "dev", "run-b")
    assert [row.run_id for row in database.rows] == ["run-a"]
    connector.release_run_lock(STATE_TABLE, "dev", "run-a")
    assert database.rows == []
    assert not connector.extend_run_lock(STATE_TABLE, "dev", claim)


def test_every_lock_value_is_bound_and_the_table_sits_beside_the_state_table() -> None:
    database = LockDatabase()
    LockConnector(database).acquire_run_lock(
        STATE_TABLE, "dev", LockClaim("run'; DROP TABLE x; --", "R", "h"), break_stale=False
    )
    assert all("DROP TABLE x" not in statement for statement in database.statements)
    assert all("SST_STATE_LOCK" in statement for statement in database.statements)
    assert database.rows[0].run_id == "run'; DROP TABLE x; --"
