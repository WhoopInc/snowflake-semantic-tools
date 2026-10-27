"""`RecoverState`: which unrecorded objects a marker proves, and the entries it records for them."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.recover import RecoverState, classify
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ABSENT,
    NO_MARKER,
    OTHER_DEFINITION,
    RECORDED,
    ROW_DIFFERS,
    UNMARKED,
    UNOBSERVABLE,
    ChangeReason,
    OwnershipMarker,
    RecoveryProof,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import applied_entry, change, changeset, empty_state, rendered
from tests.helpers.clocks import FixedClock
from tests.helpers.recover_changes import TABLE, Proving, composite, proved, recover, unmanaged
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


PROOF = RecoveryProof(
    RECORDED,
    (("alias", "SST_A"), ("version", "VERSION$1")),
    (("CORTEX EXTENSION", "DB.SCHEMA.S"), ("STAGE", "DB.SCHEMA.BUNDLES")),
    "DB.SCHEMA.S",
)


def test_a_composite_its_handler_proves_is_recorded_though_dependency_blocking_overwrote_its_reason() -> None:
    port = FakeSnowflake()
    planned = composite("S")
    assert planned.reason is ChangeReason.DEPENDENCY_BLOCKED and planned.observed is None
    result = recover(port, planned, handlers={"skill": Proving(port, {planned.key: PROOF})})
    assert result.recorded == (planned.key,)
    entry = (port.remote_state or {})[planned.key]
    # The entry apply records for a publish of the definition, from the manifest planned.
    assert (entry.outcome, entry.fingerprint, entry.ddl_sha256) == ("observed",) + (rendered("S").fingerprint,) * 2
    assert (entry.qualified_name, entry.manifest_id) == (rendered("S").target.sql, "m" * 64)
    assert entry.component_fingerprints == PROOF.component_fingerprints
    assert [(item.object_type, item.qualified_name) for item in entry.applied_resources] == list(
        PROOF.physical_resources
    )
    assert port.state_manifest == "m" * 64


def test_the_state_manifest_is_the_one_most_markers_name_and_proofs_do_not_vote() -> None:
    port = FakeSnowflake()
    skills = [composite(name) for name in ("S", "T", "U")]
    handler = Proving(port, {item.key: PROOF for item in skills})
    result = recover(port, proved("A"), *skills, handlers={"skill": handler})
    assert len(result.recorded) == 4
    # Three proofs name the planned manifest; the one marker names the apply that published it.
    assert port.state_manifest == "a" * 64


def test_a_composite_its_handler_cannot_prove_is_reported_with_why_and_a_failed_read_proves_nothing() -> None:
    port = FakeSnowflake()
    differs, unread = composite("S"), composite("T")
    answers: dict[str, RecoveryProof | SnowflakePortError] = {
        differs.key: RecoveryProof(ROW_DIFFERS, qualified_name="DB.SCHEMA.R row 's'"),
        unread.key: SnowflakePortError("x"),
    }
    result = recover(port, differs, unread, handlers={"skill": Proving(port, answers)})
    found = {item.key: (item.found, item.qualified_name, item.next_apply) for item in result.artifacts}
    assert found == {
        differs.key: (ROW_DIFFERS, "DB.SCHEMA.R row 's'", "blocked"),
        unread.key: (UNOBSERVABLE, rendered("T").target.sql, "blocked"),
    }
    assert not result.wrote and [item.code for item in result.diagnostics] == ["SST-APL032", "SST-APL033", "SST-APL033"]


def test_a_composite_without_a_handler_or_already_recorded_is_not_asked_to_prove_itself() -> None:
    port = FakeSnowflake()
    planned = composite("S")
    [item] = recover(port, planned).artifacts
    assert item.found == UNMARKED
    recorded = empty_state({planned.key: applied_entry("DB.SCHEMA.S", "f" * 64)})
    assert classify(planned, recorded) is None


def test_a_proved_composite_an_apply_recorded_since_the_plan_is_left_as_it_is() -> None:
    port = FakeSnowflake(state={"semantic_view:s": applied_entry("DB.SCHEMA.S", "e" * 64)})
    planned = composite("S")
    result = recover(port, planned, handlers={"skill": Proving(port, {planned.key: PROOF})})
    assert result.recorded == () and (port.remote_state or {})[planned.key].fingerprint == "e" * 64


def test_an_artifact_the_planned_state_records_is_left_out_of_what_recover_reports() -> None:
    port = FakeSnowflake()
    planned = empty_state({"semantic_view:a": applied_entry("DB.SCHEMA.A", "f" * 64)})
    result = RecoverState(port, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(
        changeset(proved("A"), unmanaged("B", None)), planned, "verify"
    )
    assert [item.key for item in result.artifacts] == ["semantic_view:b"] and not result.wrote
