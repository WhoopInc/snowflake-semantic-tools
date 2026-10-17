"""The run lock and state writes on hybrid tables, against a double with Snowflake's measured row locking.

`HybridTables` fails a test outright for any statement that would wait on real Snowflake, so
every test here also proves the protocol never issues one.
"""

from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import lock_table
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import (
    STANDARD_TABLES,
    LockAcquisition,
    LockClaim,
    LockFence,
    StateWrite,
)
from tests.helpers.hybrid_lock_tables import RIVAL, HybridTables, HybridTablesConnector
from tests.helpers.lock_tables import LOCK, STATE

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
ENTRY = AppliedEntry("f" * 64, "DB.S.V", "2026-09-29T00:00:00Z", "run", "applied", "d" * 64, "m" * 64)


def claim(
    tables: HybridTables, run_id: str, target: str = "dev", *, break_stale: bool = False, ttl: int = 60
) -> LockAcquisition:
    return HybridTablesConnector(tables).acquire_run_lock(
        STATE_TABLE, target, LockClaim(run_id, "ROLE", "laptop", ttl), break_stale=break_stale
    )


def fence_of(acquisition: LockAcquisition) -> LockFence:
    assert acquisition.acquired and acquisition.fence is not None
    return acquisition.fence


@pytest.fixture(autouse=True)
def no_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.lock_table.time.sleep", lambda seconds: None
    )


def test_a_claim_seeds_the_targets_row_outside_any_transaction_then_takes_it_under_for_update() -> None:
    tables = HybridTables()
    won = claim(tables, "run-a")
    assert fence_of(won) == LockFence("run-a", 1) and won.holder is None
    statements = tables.executed()
    seed = next(index for index, statement in enumerate(statements) if statement.startswith(f"INSERT INTO {LOCK}"))
    begin = statements.index("BEGIN")
    assert seed < begin, "the row is seeded autocommitted, before the transaction"
    assert statements[begin + 1].endswith("FOR UPDATE")
    # Expiry is read once the row is locked: Snowflake refuses CURRENT_TIMESTAMP() in a FOR UPDATE.
    assert statements[begin + 2].startswith("SELECT EXPIRES_AT <= CURRENT_TIMESTAMP()")
    assert statements[begin + 3].startswith(f"UPDATE {LOCK} SET RUN_ID = %s")
    assert not any("MERGE" in statement or "MAX(GENERATION)" in statement for statement in statements)
    assert tables.rows["dev"].run_id == "run-a"


def test_a_live_lock_is_refused_and_the_rival_seed_of_an_existing_row_is_ignored() -> None:
    tables = HybridTables()
    claim(tables, "run-a")
    lost = claim(tables, "run-b")
    assert not lost.acquired and lost.holder is not None and lost.holder.run_id == "run-a"
    assert tables.statements[-1][1] == "ROLLBACK"
    assert tables.rows["dev"].run_id == "run-a"


def test_targets_lock_separately_so_one_never_waits_on_another() -> None:
    tables = HybridTables()
    tables.hold("prod")
    assert fence_of(claim(tables, "run-dev", "dev")) == LockFence("run-dev", 1)
    assert fence_of(claim(tables, "run-qa", "qa")) == LockFence("run-qa", 1)


def test_an_expired_lock_is_broken_only_on_request_and_issues_the_next_generation() -> None:
    tables = HybridTables()
    claim(tables, "run-a", ttl=10)
    tables.now = 20.0
    refused = claim(tables, "run-b")
    assert not refused.acquired and refused.holder is not None and refused.holder.expired
    broken = claim(tables, "run-b", break_stale=True)
    assert broken.broke_stale and fence_of(broken) == LockFence("run-b", 2)


def test_a_release_frees_the_row_and_keeps_its_generation_so_the_next_claim_issues_a_later_one() -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    first = fence_of(claim(tables, "run-a"))
    connector.release_run_lock(STATE_TABLE, "dev", first)
    row = tables.rows["dev"]
    assert (row.run_id, row.owner, row.generation) == ("", None, 1)
    assert not connector.verify_run_lock(STATE_TABLE, "dev", first)
    assert fence_of(claim(tables, "run-b")) == LockFence("run-b", 2)
    # A stale fence releases nothing.
    connector.release_run_lock(STATE_TABLE, "dev", first)
    assert tables.rows["dev"].run_id == "run-b"


def test_an_extension_holds_only_for_the_current_fence() -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    fence = fence_of(claim(tables, "run-a", ttl=10))
    tables.now = 5.0
    assert connector.extend_run_lock(STATE_TABLE, "dev", fence, 100)
    assert tables.rows["dev"].expires_at == 105.0
    assert not connector.extend_run_lock(STATE_TABLE, "dev", LockFence("run-a", 9), 100)


def test_a_row_a_rival_holds_is_asked_for_again_until_it_is_free() -> None:
    tables = HybridTables()
    fence = fence_of(claim(tables, "run-a"))
    tables.hold("dev")
    asked: list[str] = []

    def on(event: str, statement: str, connection: int) -> None:
        if event == "refused":
            asked.append(statement)
            if len(asked) == 3:
                tables.let_go()

    tables.on = on
    assert HybridTablesConnector(tables).extend_run_lock(STATE_TABLE, "dev", fence, 100)
    assert len(asked) == 3


def test_a_row_that_stays_held_fails_the_operation_with_the_transaction_rolled_back() -> None:
    tables = HybridTables()
    fence = fence_of(claim(tables, "run-a"))
    tables.hold("dev")
    with pytest.raises(SnowflakePortError) as raised:
        HybridTablesConnector(tables).release_run_lock(STATE_TABLE, "dev", fence)
    assert raised.value.errno == lock_table.KEY_LOCKED
    assert tables.statements[-1][1] == "ROLLBACK"
    refused = [statement for statement in tables.executed() if statement.endswith("FOR UPDATE")]
    assert len(refused) >= lock_table._ROW_LOCK_ATTEMPTS


def test_a_state_write_replaces_changed_entries_and_stamps_only_while_the_fence_holds() -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    fence = fence_of(claim(tables, "run-a"))
    tables.committed.state[("dev", "semantic_view:old")] = ("dev", "semantic_view:old")
    tables.committed.state[("dev", "semantic_view:v")] = ("dev", "semantic_view:v", "stale")
    write = StateWrite(MappingProxyType({"semantic_view:v": ENTRY}), deletes=("semantic_view:old",))
    assert connector.write_state(STATE_TABLE, "dev", "m" * 64, write, fence)
    assert set(tables.committed.state) == {("dev", "semantic_view:v")}
    assert tables.committed.state[("dev", "semantic_view:v")][2] == "f" * 64
    assert tables.committed.manifests == {"dev": "m" * 64}
    written = [statement for statement in tables.executed() if STATE in statement and "_LOCK" not in statement]
    assert [statement.split(" ", 1)[0] for statement in written] == ["DELETE", "INSERT", "UPDATE"]
    assert not any("MERGE" in statement for statement in written)
    # A broken lock writes nothing.
    tables.now = 1000.0
    fence_of(claim(tables, "run-b", break_stale=True))
    assert not connector.write_state(STATE_TABLE, "dev", "n" * 64, write, fence)
    assert tables.committed.manifests == {"dev": "m" * 64}


def test_many_entries_are_written_in_batches() -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    fence = fence_of(claim(tables, "run-a"))
    write = StateWrite(MappingProxyType({f"semantic_view:v{index:03}": ENTRY for index in range(120)}))
    assert connector.write_state(STATE_TABLE, "dev", "m" * 64, write, fence)
    assert len(tables.committed.state) == 120
    inserts = [statement for statement in tables.executed() if statement.startswith(f"INSERT INTO {STATE} ")]
    deletes = [statement for statement in tables.executed() if statement.startswith(f"DELETE FROM {STATE} ")]
    assert (len(inserts), len(deletes)) == (3, 3)


def test_a_table_of_the_other_kind_is_refused_naming_both_kinds() -> None:
    tables = HybridTables(hybrid=False)
    with pytest.raises(SnowflakePortError) as raised:
        claim(tables, "run-a")
    assert raised.value.diagnostic is not None and raised.value.diagnostic.code == "SST-APL031"
    assert (
        raised.value.diagnostic.message == "DB.S.SST_STATE_LOCK is a standard table, and state.+table_kind says hybrid"
    )
    assert not any(statement == "BEGIN" for statement in tables.executed())
    # A standard connector on a hybrid table is refused the same way.
    hybrid = HybridTables()
    standard = HybridTablesConnector(hybrid, kind=STANDARD_TABLES)
    with pytest.raises(SnowflakePortError) as other:
        standard.check_table_kind(QualifiedName.parse(LOCK))
    assert other.value.diagnostic is not None and other.value.diagnostic.context["found"] == "hybrid"


def test_the_preflight_reads_each_kind_and_never_raises() -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    lock = QualifiedName.parse(LOCK)
    assert connector.table_kind(lock) == "hybrid"
    tables.hybrid = False
    assert connector.table_kind(lock) == "standard"
    # Absent, and unreadable: neither is known to be the wrong kind.
    tables.kind_listed = False
    assert connector.table_kind(lock) is None
    tables.kind_refused = True
    assert connector.table_kind(lock) is None


def test_the_kind_is_read_once_per_table_and_session() -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    connector.check_table_kind(QualifiedName.parse(LOCK))
    connector.check_table_kind(QualifiedName.parse(LOCK))
    assert sum(statement.startswith("SELECT IS_HYBRID") for statement in tables.executed()) == 1


def test_a_hybrid_table_a_transient_schema_refuses_is_named() -> None:
    tables = HybridTables()
    tables.lock_table_exists = False
    tables.create_fails = 391412
    with pytest.raises(SnowflakePortError) as raised:
        claim(tables, "run-a")
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None and diagnostic.code == "SST-APL030"
    assert (
        diagnostic.message == "DB.S.SST_STATE_LOCK: a hybrid table cannot be created in a transient database or schema"
    )
    assert any(statement.startswith(f"CREATE HYBRID TABLE IF NOT EXISTS {LOCK}") for statement in tables.executed())


def test_a_target_with_no_row_cannot_be_claimed_inside_the_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    tables = HybridTables()
    connector = HybridTablesConnector(tables)
    # As if the seed were lost: the transaction finds no row, and says so rather than inserting one.
    monkeypatch.setattr(
        connector, "_ensure_lock_table", lambda state_table, target_name="": lock_table.lock_table_sql(state_table)
    )
    with pytest.raises(SnowflakePortError, match="no row for target 'dev'"):
        connector.acquire_run_lock(STATE_TABLE, "dev", LockClaim("run-a", "ROLE", "laptop", 60), break_stale=False)
    assert tables.statements[-1][1] == "ROLLBACK"
    assert RIVAL not in {who for who, _ in tables.statements}
