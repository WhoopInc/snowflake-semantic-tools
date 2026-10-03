"""The run lock against a driver double with Snowflake's transactional locking.

`LockTables` runs each lock operation the connector sends as Snowflake would: statement-level
READ COMMITTED, and UPDATE, DELETE and MERGE locks held until COMMIT or ROLLBACK, judged
against what they read before waiting. The races are staged deterministically: one session
is held right after a statement while a rival session is observed blocking on the table lock.
"""

from __future__ import annotations

import threading
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, StateWrite
from tests.helpers.lock_tables import LockRow, LockTables, LockTablesConnector

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
ENTRY = AppliedEntry("f" * 64, "DB.S.V", "2026-09-29T00:00:00Z", "run", "applied", "d" * 64, "m" * 64)
WRITE = StateWrite(MappingProxyType({"semantic_view:v": ENTRY}))


def claim(tables: LockTables, run_id: str, *, break_stale: bool = False, ttl: int = 60) -> LockAcquisition:
    return LockTablesConnector(tables).acquire_run_lock(
        STATE_TABLE, "dev", LockClaim(run_id, "ROLE", "laptop", ttl), break_stale=break_stale
    )


def fence_of(acquisition: LockAcquisition) -> LockFence:
    assert acquisition.acquired and acquisition.fence is not None
    return acquisition.fence


class Race:
    """Hold `holder`'s session just after it ran `after`, until `rival`'s session blocks on the lock table."""

    def __init__(self, tables: LockTables, after: str) -> None:
        self.tables = tables
        self.after = after
        self.held = threading.Event()
        self.rival_waiting = threading.Event()
        self.holder: int | None = None
        tables.on = self._on

    def _on(self, event: str, statement: str, connection: int) -> None:
        if event == "waiting" and connection != self.holder:
            self.rival_waiting.set()
        elif event == "ran" and self.holder is None and statement.startswith(self.after):
            self.holder = connection
            self.held.set()
            assert self.rival_waiting.wait(timeout=5), "the rival never blocked on the lock table"

    def run(self, first: object, second: object) -> None:
        """Run `first` on its own thread until it is held, then `second`, then let both finish."""
        assert callable(first) and callable(second)
        threads = [threading.Thread(target=first)]
        threads[0].start()
        assert self.held.wait(timeout=5)
        threads.append(threading.Thread(target=second))
        threads[1].start()
        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()


def test_a_free_lock_is_claimed_with_the_first_generation_and_records_the_claim() -> None:
    tables = LockTables()
    acquisition = claim(tables, "run-a")
    assert (acquisition.acquired, acquisition.holder, acquisition.broke_stale) == (True, None, False)
    assert acquisition.fence == LockFence("run-a", 1)
    assert [(row.run_id, row.owner, row.host, row.expires_at, row.generation) for row in tables.rows] == [
        ("run-a", "ROLE", "laptop", 60.0, 1)
    ]


def test_every_lock_transaction_writes_the_mutex_row_before_it_reads_anything() -> None:
    tables = LockTables()
    fence = fence_of(claim(tables, "run-a"))
    connector = LockTablesConnector(tables)
    connector.extend_run_lock(STATE_TABLE, "dev", fence, 60)
    connector.write_state(STATE_TABLE, "dev", "m" * 64, WRITE, fence)
    connector.release_run_lock(STATE_TABLE, "dev", fence)
    statements = tables.executed()
    begins = [index for index, statement in enumerate(statements) if statement == "BEGIN"]
    assert len(begins) == 4
    for begin in begins:
        assert (
            statements[begin + 1]
            == "UPDATE DB.S.SST_STATE_LOCK SET ACQUIRED_AT = CURRENT_TIMESTAMP() WHERE TARGET_NAME = %s"
        )


def test_a_live_lock_is_refused_and_names_its_holder_even_with_break_stale() -> None:
    tables = LockTables()
    claim(tables, "run-a")
    refused = claim(tables, "run-b", break_stale=True)
    assert not refused.acquired and refused.fence is None and refused.holder is not None
    assert refused.holder.describe() == "run run-a (ROLE on laptop), expires 2026-10-01T00:01:00Z"
    assert [row.run_id for row in tables.rows] == ["run-a"]
    assert tables.executed()[-1] == "ROLLBACK"


def test_an_expired_lock_is_taken_over_only_with_break_stale_under_a_later_generation() -> None:
    tables = LockTables()
    claim(tables, "run-a", ttl=10)
    tables.now = 11.0
    kept = claim(tables, "run-b")
    assert not kept.acquired and kept.holder is not None and kept.holder.expired
    broke = claim(tables, "run-b", break_stale=True)
    assert broke.acquired and broke.broke_stale and broke.holder is not None
    assert broke.holder.run_id == "run-a" and broke.fence == LockFence("run-b", 2)
    assert [(row.run_id, row.generation) for row in tables.rows] == [("run-b", 2)]


def test_generations_keep_rising_after_a_release() -> None:
    tables = LockTables()
    connector = LockTablesConnector(tables)
    connector.release_run_lock(STATE_TABLE, "dev", fence_of(claim(tables, "run-a")))
    assert tables.rows == [] and claim(tables, "run-b").fence == LockFence("run-b", 2)


def test_two_runs_racing_for_a_free_lock_leave_exactly_one_holder() -> None:
    # The first claim holds the table right after its mutex write; the second blocks behind it,
    # so it reads the first's committed row rather than the free lock both would otherwise see.
    tables = LockTables()
    race = Race(tables, "UPDATE DB.S.SST_STATE_LOCK SET ACQUIRED_AT")
    results: dict[str, LockAcquisition] = {}
    race.run(
        lambda: results.setdefault("run-a", claim(tables, "run-a")),
        lambda: results.setdefault("run-b", claim(tables, "run-b")),
    )
    assert results["run-a"].acquired and not results["run-b"].acquired
    assert results["run-b"].holder is not None and results["run-b"].holder.run_id == "run-a"
    assert [row.run_id for row in tables.rows] == ["run-a"]


def test_two_runs_racing_to_break_one_expired_lock_leave_exactly_one_holder() -> None:
    tables = LockTables()
    claim(tables, "old", ttl=1)
    tables.now = 5.0
    # Held after reading the expired row: the rival's claim cannot read it until this one commits.
    race = Race(tables, "SELECT RUN_ID, OWNER, HOST")
    results: dict[str, LockAcquisition] = {}
    race.run(
        lambda: results.setdefault("run-a", claim(tables, "run-a", break_stale=True)),
        lambda: results.setdefault("run-b", claim(tables, "run-b", break_stale=True)),
    )
    assert results["run-a"].acquired and results["run-a"].broke_stale
    assert not results["run-b"].acquired and results["run-b"].holder is not None
    assert results["run-b"].holder.run_id == "run-a" and not results["run-b"].holder.expired
    assert [row.run_id for row in tables.rows] == ["run-a"]


def test_the_fenced_state_write_of_a_run_whose_lock_was_broken_is_refused() -> None:
    tables = LockTables()
    loser = fence_of(claim(tables, "loser", ttl=1))
    tables.now = 5.0
    winner = fence_of(claim(tables, "winner", break_stale=True))
    connector = LockTablesConnector(tables)
    assert not connector.write_state(STATE_TABLE, "dev", "l" * 64, WRITE, loser)
    assert tables.committed.state == {} and tables.executed()[-1] == "ROLLBACK"
    assert connector.write_state(STATE_TABLE, "dev", "w" * 64, WRITE, winner)
    assert list(tables.committed.state) == [("dev", "semantic_view:v")]
    assert tables.committed.manifests == {"dev": "w" * 64}


def test_a_takeover_racing_a_state_write_lands_wholly_after_it() -> None:
    # The write holds the table after checking its fence; the takeover waits, then refuses the
    # loser's next write. Neither interleaves with the other.
    tables = LockTables()
    loser = fence_of(claim(tables, "loser", ttl=1))
    tables.now = 5.0
    race = Race(tables, "SELECT RUN_ID, GENERATION")
    written: list[bool] = []
    taken: list[LockAcquisition] = []
    race.run(
        lambda: written.append(LockTablesConnector(tables).write_state(STATE_TABLE, "dev", "l" * 64, WRITE, loser)),
        lambda: taken.append(claim(tables, "winner", break_stale=True)),
    )
    assert written == [True] and taken[0].acquired
    assert tables.committed.manifests == {"dev": "l" * 64}
    assert not LockTablesConnector(tables).write_state(STATE_TABLE, "dev", "x" * 64, WRITE, loser)
    assert tables.committed.manifests == {"dev": "l" * 64}


def test_a_state_write_racing_a_takeover_is_refused_when_the_takeover_commits_first() -> None:
    tables = LockTables()
    loser = fence_of(claim(tables, "loser", ttl=1))
    tables.now = 5.0
    race = Race(tables, "SELECT RUN_ID, OWNER, HOST")
    taken: list[LockAcquisition] = []
    written: list[bool] = []
    race.run(
        lambda: taken.append(claim(tables, "winner", break_stale=True)),
        lambda: written.append(LockTablesConnector(tables).write_state(STATE_TABLE, "dev", "l" * 64, WRITE, loser)),
    )
    assert taken[0].acquired and written == [False]
    assert tables.committed.state == {} and tables.committed.manifests == {}


def test_extend_and_release_touch_only_the_fenced_row() -> None:
    tables = LockTables()
    connector = LockTablesConnector(tables)
    fence = fence_of(claim(tables, "run-a", ttl=10))
    tables.now = 5.0
    assert connector.extend_run_lock(STATE_TABLE, "dev", fence, 10)
    assert tables.rows[0].expires_at == 15.0
    assert not connector.extend_run_lock(STATE_TABLE, "dev", LockFence("run-a", 9), 10)
    assert not connector.extend_run_lock(STATE_TABLE, "dev", LockFence("run-b", 1), 10)
    connector.release_run_lock(STATE_TABLE, "dev", LockFence("run-a", 9))
    assert [row.run_id for row in tables.rows] == ["run-a"]
    connector.release_run_lock(STATE_TABLE, "dev", fence)
    assert tables.rows == []
    assert not connector.extend_run_lock(STATE_TABLE, "dev", fence, 10)


def test_rows_from_before_fences_are_judged_and_cleared_by_a_takeover() -> None:
    # An older table may hold two rows for one target, and no generation; both expired, both go.
    tables = LockTables()
    claim(tables, "seed")
    tables.committed.locks = [row for row in tables.committed.locks if not row.target]
    for run_id in ("old-1", "old-2"):
        tables.committed.locks.append(LockRow("dev", run_id, None, None, 0.0, 1.0, None))
    tables.now = 5.0
    broke = claim(tables, "new", break_stale=True)
    assert broke.acquired and broke.holder is not None and broke.holder.run_id == "old-1"
    assert [row.run_id for row in tables.rows] == ["new"]


def test_a_lock_transaction_without_a_mutex_row_refuses_and_rolls_back() -> None:
    tables = LockTables()
    with pytest.raises(SnowflakePortError, match="no mutex row"):
        LockTablesConnector(tables).extend_run_lock(STATE_TABLE, "dev", LockFence("run-a", 1), 10)
    assert tables.executed()[-1] == "ROLLBACK"


def test_a_lock_needs_a_target_name() -> None:
    with pytest.raises(ValueError, match="target name"):
        LockTablesConnector(LockTables()).acquire_run_lock(STATE_TABLE, "", LockClaim("run"), break_stale=False)


def test_every_lock_value_is_bound_and_the_table_sits_beside_the_state_table() -> None:
    tables = LockTables()
    acquisition = claim(tables, "run'; DROP TABLE x; --")
    assert all("DROP TABLE x" not in statement for statement in tables.executed())
    assert all("SST_STATE_LOCK" in statement for statement in tables.executed() if statement not in ("BEGIN", "COMMIT"))
    assert tables.rows[0].run_id == "run'; DROP TABLE x; --" and acquisition.acquired
