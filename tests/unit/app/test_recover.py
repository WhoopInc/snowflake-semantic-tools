"""`RecoverState`: which unrecorded objects a marker proves, and the entries it records for them."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.recover import ABSENT, NO_MARKER, OTHER_DEFINITION, RECORDED, UNMARKED, classify
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import applied_entry, change, empty_state, rendered
from tests.helpers.recover_changes import TABLE, proved, recover, unmanaged
from tests.helpers.snowflake_fake import FakeSnowflake


def test_only_an_unrecorded_object_whose_marker_names_the_compiled_definition_is_recorded() -> None:
    trusted = proved("A")
    stranger = unmanaged("B", None)
    other = unmanaged("C", OwnershipMarker("a" * 64, "f" * 64))
    absent = change(rendered("D"))
    composite = replace(change(rendered("E")), composite_observation=object())  # type: ignore[arg-type]
    found = {c.key: classify(c, empty_state()) for c in (trusted, stranger, other, absent, composite)}
    assert {key: item.found for key, item in found.items() if item is not None} == {
        "semantic_view:a": RECORDED,
        "semantic_view:b": NO_MARKER,
        "semantic_view:c": OTHER_DEFINITION,
        "semantic_view:d": ABSENT,
        "semantic_view:e": UNMARKED,
    }
    # An artifact state already records is not recover's to judge.
    recorded = empty_state({"semantic_view:a": applied_entry("DB.SCHEMA.A", "f" * 64)})
    assert classify(trusted, recorded) is None


def test_a_marker_under_another_name_proves_nothing() -> None:
    moved = proved("A")
    assert moved.observed is not None
    elsewhere = replace(moved, observed=replace(moved.observed, qualified_name=QualifiedName.parse("DB.SCHEMA.OTHER")))
    item = classify(elsewhere, empty_state())
    assert item is not None and item.found == OTHER_DEFINITION


def test_recover_records_the_proved_entries_under_the_lock_as_apply_records_an_observed_object() -> None:
    port = FakeSnowflake()
    store = InMemoryStateStore(empty_state())
    result = recover(port, proved("A"), unmanaged("B", None), store=store)
    assert result.recorded == ("semantic_view:a",)
    entry = (port.remote_state or {})["semantic_view:a"]
    assert (entry.outcome, entry.fingerprint, entry.manifest_id) == ("observed", rendered("A").fingerprint, "a" * 64)
    assert entry.qualified_name == rendered("A").target.sql
    assert set(port.remote_state or {}) == {"semantic_view:a"}
    # The local cache gains it too, and the lock was released.
    assert "semantic_view:a" in store.read_local().applied  # type: ignore[union-attr]
    assert port.run_locks.acquire_run_lock(TABLE, "verify", LockClaim("next"), break_stale=False).acquired
    codes = [(item.code, item.context.get("count"), item.context.get("artifact")) for item in result.diagnostics]
    assert codes == [("SST-APL032", 1, None), ("SST-APL033", None, "semantic_view:b")]


def test_an_entry_an_apply_recorded_since_the_plan_is_left_as_it_is() -> None:
    port = FakeSnowflake(state={"semantic_view:a": applied_entry("DB.SCHEMA.A", "e" * 64)})
    result = recover(port, proved("A"))
    assert result.recorded == ()
    assert (port.remote_state or {})["semantic_view:a"].fingerprint == "e" * 64


def test_nothing_to_record_takes_no_lock_and_writes_nothing() -> None:
    port = FakeSnowflake()
    result = recover(port, unmanaged("B", None))
    assert not result.wrote and result.recorded == ()
    assert port.remote_state in (None, {})
    assert [item.code for item in result.diagnostics] == ["SST-APL032", "SST-APL033"]


def test_a_held_lock_refuses_the_recover_with_nothing_written() -> None:
    port = FakeSnowflake()
    assert port.run_locks.acquire_run_lock(
        TABLE, "verify", LockClaim("rival", ttl_seconds=600), break_stale=False
    ).acquired
    result = recover(port, proved("A"))
    assert result.refused and not result.wrote
    assert port.remote_state in (None, {})
    assert "SST-APL011" in [item.code for item in result.diagnostics]


def test_a_lock_lost_before_the_write_records_nothing_and_says_it_was_refused() -> None:
    port = FakeSnowflake()
    port.write_state = lambda *args, **kwargs: False  # type: ignore[method-assign]
    result = recover(port, proved("A"))
    assert result.refused and not result.wrote
    assert "SST-APL032" not in [item.code for item in result.diagnostics]


def test_a_live_object_the_plan_does_not_call_unmanaged_has_no_marker_recover_can_use() -> None:
    # Such as a profile registry row: observed, but not as an object SHOW lists with a comment.
    artifact = rendered("P")
    planned = change(artifact, live=None)
    live = replace(planned, observed=proved("P").observed)
    item = classify(live, empty_state())
    assert item is not None and item.found == UNMARKED and item.next_apply == "create"
