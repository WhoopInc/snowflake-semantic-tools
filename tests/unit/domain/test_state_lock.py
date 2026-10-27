"""The run lock's values and the state delta a run writes."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry
from snowflake_semantic_tools.domain.state.lock import RunLock, lock_table_for, state_write


def entry(fingerprint: str) -> AppliedEntry:
    return AppliedEntry(fingerprint, "DB.S.V", "now", "run", APPLIED, fingerprint, "m")


def test_the_lock_table_sits_beside_the_state_table_and_keeps_its_quoting() -> None:
    assert lock_table_for(QualifiedName.parse("DB.S.SST_STATE")).sql == "DB.S.SST_STATE_LOCK"
    assert lock_table_for(QualifiedName.parse('DB.S."state"')).sql == 'DB.S."state_LOCK"'


def test_a_holder_is_described_by_run_then_who_and_where_when_known() -> None:
    assert RunLock("r1", "ROLE", "ci", "t0", "t1", False).describe() == "run r1 (ROLE on ci), expires t1"
    assert RunLock("r1", "", "ci", "t0", "t1", True).describe() == "run r1 (ci), expired t1"
    assert RunLock("r1", "", "", "t0", "t1", True).describe() == "run r1, expired t1"


def test_a_state_write_holds_only_changed_entries_and_removed_keys() -> None:
    before = {"same": entry("a"), "changed": entry("a"), "gone": entry("a")}
    after = {"same": entry("a"), "changed": entry("b"), "new": entry("c")}
    write = state_write(before, after)
    assert dict(write.upserts) == {"changed": entry("b"), "new": entry("c")}
    assert list(write.upserts) == ["changed", "new"]
    assert write.deletes == ("gone",)
    unchanged = state_write(after, after)
    assert dict(unchanged.upserts) == {} and unchanged.deletes == ()
