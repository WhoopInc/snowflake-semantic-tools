from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts, classify_error, preserves_grants
from snowflake_semantic_tools.app.apply.errors import _outcome_diagnostic
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    ClassifiedError,
    ErrorKind,
    ExecResult,
    ExecutionError,
    FailurePolicy,
    GrantCheck,
    GrantRow,
    OutcomeStatus,
    OwnershipMarker,
    RetryPolicy,
    ShowRow,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
from snowflake_semantic_tools.domain.state import FAILED_AFTER_WRITE
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore, failed
from tests.helpers.artifact_builders import change, changeset, manifest, marker, observed, rendered, state, target


def runner(
    port: InMemorySnowflake | None = None, store: InMemoryStateStore | None = None, clock: FixedClock | None = None
):
    port = port or InMemorySnowflake()
    store = store or InMemoryStateStore()
    clock = clock or FixedClock()
    return (
        ApplyArtifacts(port, store, clock, state_table=rendered("STATE").target, actor="TEST_ROLE"),
        port,
        store,
        clock,
    )


@pytest.mark.parametrize(
    ("message", "sqlstate", "kind", "retryable"),
    [
        ("not authorized", None, ErrorKind.PRIVILEGE, False),
        ("missing does not exist", None, ErrorKind.NOT_FOUND, False),
        ("syntax error", None, ErrorKind.SYNTAX, False),
        ("timeout", None, ErrorKind.TRANSIENT, True),
        ("other", None, ErrorKind.UNKNOWN, False),
        ("x", "28000", ErrorKind.PRIVILEGE, False),
        ("x", "02000", ErrorKind.NOT_FOUND, False),
        ("x", "42000", ErrorKind.SYNTAX, False),
        ("x", "08001", ErrorKind.TRANSIENT, True),
    ],
)
def test_error_classification(message: str, sqlstate: str | None, kind: ErrorKind, retryable: bool) -> None:
    value = classify_error(message, sqlstate=sqlstate)
    assert value.kind is kind and value.retryable is retryable


def test_a_name_conflict_is_not_classified_as_a_syntax_error() -> None:
    assert classify_error("Object 'X' already exists.").code == "SST-SNO002"
    assert classify_error("x", sqlstate="42710").code == "SST-SNO002"
    assert classify_error("x", sqlstate="42000").code == "SST-SNO009"


def test_apply_create_writes_remote_then_local_state() -> None:
    artifact = rendered()
    use_case, port, store, _ = runner()
    result = use_case.run(changeset(change(artifact)), state())
    assert result.success and result.state_written
    assert result.outcomes[0].status is OutcomeStatus.APPLIED
    assert port.scripts == [artifact.statements]
    assert store.writes and artifact.key in store.writes[0].applied
    assert artifact.key in (port.remote_state or {})


def test_remote_state_failure_does_not_publish_uncommitted_local_state() -> None:
    artifact = rendered()
    port = InMemorySnowflake()
    store = InMemoryStateStore()

    def fail_state(*args, **kwargs):
        del args, kwargs
        raise SnowflakePortError("state table unavailable")

    port.write_state = fail_state  # type: ignore[method-assign]
    use_case, _, _, _ = runner(port, store)

    with pytest.raises(SnowflakePortError, match="state table unavailable"):
        use_case.run(changeset(change(artifact)), state())

    assert port.scripts == [artifact.statements]
    assert store.state is None


def test_apply_create_refuses_object_that_appears_after_plan() -> None:
    artifact = rendered()
    port = InMemorySnowflake()
    port.existing = {
        artifact.target.sql,
        *(relation.sql for relation in artifact.required_relations),
    }
    use_case, _, store, _ = runner(port)

    result = use_case.run(changeset(change(artifact)), state())

    assert not result.success
    assert result.outcomes[0].error.code == "SST-APL012"  # type: ignore[union-attr]
    assert port.scripts == []
    assert store.state is not None
    assert artifact.key not in store.state.applied


def test_apply_refuses_blocked_and_live_lock() -> None:
    artifact = rendered()
    blocked = replace(
        change(artifact),
        action=Action.BLOCKED,
        diagnostics=DiagnosticBag((D("SST-REF001", model="x"),)),
    )
    use_case, _, _, _ = runner()
    refused = use_case.run(changeset(blocked), state())
    assert not refused.state_written and refused.diagnostics[-1].code == "SST-APL003"

    store = InMemoryStateStore()
    store.locked = True
    store.holder = "other"
    use_case, _, _, _ = runner(store=store)
    locked = use_case.run(changeset(change(artifact)), state())
    assert locked.diagnostics[-1].code == "SST-APL011"


def test_apply_refuses_changeset_with_observation_errors_before_lock_or_write() -> None:
    artifact = rendered()
    changes = replace(
        changeset(change(artifact)), diagnostics=DiagnosticBag((D("SST-PLN001", value="x", detail="down"),))
    )
    use_case, port, store, _ = runner()
    result = use_case.run(changes, state())
    assert not result.state_written
    assert port.scripts == []
    assert not store.locked


def test_apply_breaks_stale_lock_and_retries_transient_failure() -> None:
    artifact = rendered()
    store = InMemoryStateStore()
    store.locked = True
    store.stale = True
    store.holder = "dead"
    port = InMemorySnowflake()
    port.execute_results = [failed("timeout", "08001")]
    clock = FixedClock()
    use_case, _, _, _ = runner(port, store, clock)
    result = use_case.run(
        changeset(change(artifact)),
        state(),
        ApplyOptions(break_stale_lock=True, retry=RetryPolicy(2, (7,))),
    )
    assert result.success
    assert result.diagnostics[0].code == "SST-APL010"
    assert result.outcomes[0].attempts == 2 and clock.sleeps == [7]


def test_apply_update_rechecks_marker_preserves_or_detects_grants() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    grant = GrantRow("SELECT", "ROLE", "R")
    port.grants[artifact.target.sql] = (grant,)
    use_case, _, _, _ = runner(port)
    good = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert good.success and good.outcomes[0].grants is GrantCheck.PRESERVED

    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    calls = 0

    def grants(object_type, qualified_name, routine_signature=()):
        nonlocal calls
        del object_type, qualified_name, routine_signature
        calls += 1
        return (grant,) if calls == 1 else ()

    port.show_grants = grants  # type: ignore[method-assign]
    use_case, _, _, _ = runner(port)
    lost = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert not lost.success and lost.outcomes[0].error.code == "SST-APL009"  # type: ignore[union-attr]
    assert lost.outcomes[0].write_succeeded
    assert port.remote_state is not None
    assert port.remote_state[artifact.key].fingerprint == artifact.fingerprint

    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    port.grant_error = SnowflakePortError("cannot read baseline")
    use_case, _, _, _ = runner(port)
    unreadable = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert not unreadable.success
    assert unreadable.outcomes[0].error.code == "SST-APL008"  # type: ignore[union-attr]
    assert unreadable.outcomes[0].grants is GrantCheck.UNREADABLE
    assert port.scripts == []


def test_apply_update_refuses_marker_drift_and_unsafe_replace() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = OwnershipMarker("b" * 64, artifact.fingerprint)
    use_case, _, _, _ = runner(port)
    drift = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert drift.diagnostics[-1].code == "SST-APL012"

    unsafe_artifact = replace(artifact, statements=(artifact.ddl.replace(" COPY GRANTS", ""),))
    port.markers[artifact.target.sql] = ownership
    use_case, _, _, _ = runner(port)
    unsafe = use_case.run(changeset(change(unsafe_artifact, Action.UPDATE, live=live)), state())
    assert unsafe.diagnostics[-1].code == "SST-APL004"
    assert not preserves_grants(change(unsafe_artifact, Action.UPDATE, live=live))


def test_apply_prune_requires_permission_and_marker_and_updates_state() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    prune = change(artifact, Action.PRUNE, live=live)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    from snowflake_semantic_tools.domain.state import AppliedEntry

    prior_entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "prior",
        "applied",
        artifact.fingerprint,
        ownership.manifest_id,
    )
    prior = replace(state(), applied=MappingProxyType({artifact.key: prior_entry}))
    port.remote_state = MappingProxyType({artifact.key: prior_entry})
    use_case, _, _, _ = runner(port)
    skipped = use_case.run(changeset(prune), prior)
    assert skipped.outcomes[0].status is OutcomeStatus.SKIPPED
    port.markers[artifact.target.sql] = ownership
    applied = use_case.run(changeset(prune), prior, ApplyOptions(allow_prune=True))
    assert applied.success and port.scripts[-1][0].startswith("DROP SEMANTIC VIEW")

    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    use_case, _, _, _ = runner(port)
    missing_remote_row = use_case.run(
        changeset(prune),
        prior,
        ApplyOptions(allow_prune=True),
    )
    assert missing_remote_row.success


def test_apply_preflight_and_failure_policies_account_for_every_change() -> None:
    upstream = rendered("UP")
    downstream = rendered("DOWN", depends_on=(upstream.key,))
    port = InMemorySnowflake()
    port.existing = {"OTHER"}
    use_case, _, _, _ = runner(port)
    missing = use_case.run(changeset(change(upstream), change(downstream)), state())
    assert len(missing.outcomes) == 2 and not missing.success

    port = InMemorySnowflake()
    port.execute_results = [failed("syntax error", "42000")]
    use_case, _, _, _ = runner(port)
    stopped = use_case.run(
        changeset(change(upstream), change(downstream)),
        state(),
        ApplyOptions(on_failure=FailurePolicy.STOP_DEPENDENTS),
    )
    assert [item.status for item in stopped.outcomes] == [OutcomeStatus.FAILED, OutcomeStatus.SKIPPED]
    assert {item.code for item in stopped.diagnostics} >= {"SST-APL001", "SST-APL002"}

    port = InMemorySnowflake()
    port.execute_results = [failed("syntax error", "42000")]
    use_case, _, _, _ = runner(port)
    stop_all = use_case.run(
        changeset(change(upstream), change(downstream)),
        state(),
        ApplyOptions(on_failure=FailurePolicy.STOP_ALL),
    )
    assert len(stop_all.outcomes) == 2
    assert len(port.scripts) == 1


def test_partial_apply_state_retains_observed_managed_entries() -> None:
    upstream = rendered("UP")
    downstream = rendered("DOWN", depends_on=(upstream.key,))
    ownership = marker(downstream)
    live_downstream = observed(downstream, ownership=ownership)
    port = InMemorySnowflake()
    port.execute_results = [failed("syntax error", "42000")]
    use_case, _, store, _ = runner(port)
    result = use_case.run(
        changeset(change(upstream), change(downstream, Action.UPDATE, live=live_downstream)),
        state(),
        ApplyOptions(on_failure=FailurePolicy.STOP_DEPENDENTS),
    )
    assert not result.success
    assert store.state is not None
    assert store.state.applied[downstream.key].fingerprint == ownership.fingerprint


def test_stop_dependents_propagates_skips_transitively() -> None:
    upstream = rendered("UP")
    middle = rendered("MID", depends_on=(upstream.key,))
    downstream = rendered("DOWN", depends_on=(middle.key,))
    port = InMemorySnowflake()
    port.execute_results = [failed("syntax error", "42000")]
    use_case, _, _, _ = runner(port)
    result = use_case.run(
        changeset(change(upstream), change(middle), change(downstream)),
        state(),
        ApplyOptions(on_failure=FailurePolicy.STOP_DEPENDENTS),
    )
    assert [item.status for item in result.outcomes] == [
        OutcomeStatus.FAILED,
        OutcomeStatus.SKIPPED,
        OutcomeStatus.SKIPPED,
    ]


def test_apply_covers_noop_blocked_prune_drift_and_missing_render() -> None:
    artifact = rendered()
    use_case, _, _, _ = runner()
    noop = replace(change(artifact), action=Action.NOOP)
    noop_result = use_case.run(changeset(noop), state())
    assert noop_result.outcomes[0].status is OutcomeStatus.SKIPPED
    assert not noop_result.state_written

    blocked = replace(change(artifact), action=Action.BLOCKED)
    continued = use_case.run(
        changeset(blocked),
        state(),
        ApplyOptions(on_failure=FailurePolicy.CONTINUE),
    )
    assert continued.outcomes[0].status is OutcomeStatus.SKIPPED

    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    prune = change(artifact, Action.PRUNE, live=live)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = OwnershipMarker("b" * 64, artifact.fingerprint)
    use_case, _, _, _ = runner(port)
    drift = use_case.run(changeset(prune), state(), ApplyOptions(allow_prune=True))
    assert drift.outcomes[0].error.code == "SST-APL012"  # type: ignore[union-attr]

    malformed = replace(change(artifact), rendered=None)
    use_case, _, _, _ = runner()
    failed_render = use_case.run(changeset(malformed), state())
    assert failed_render.outcomes[0].error.code == "SST-APL001"  # type: ignore[union-attr]


def test_apply_handles_unreadable_grants_and_unknown_execution_errors() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    calls = 0

    def unreadable(object_type, qualified_name, routine_signature=()):
        nonlocal calls
        del object_type, qualified_name, routine_signature
        calls += 1
        if calls == 1:
            return (GrantRow("SELECT", "ROLE", "R"),)
        raise SnowflakePortError("cannot reread")

    port.show_grants = unreadable  # type: ignore[method-assign]
    use_case, _, _, _ = runner(port)
    result = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert result.outcomes[0].grants is GrantCheck.UNREADABLE
    assert result.diagnostics[0].code == "SST-APL008"

    port = InMemorySnowflake()
    port.execute_results = [ExecResult(False)]
    use_case, _, _, _ = runner(port)
    unknown = use_case.run(changeset(change(artifact)), state())
    assert unknown.outcomes[0].error.kind is ErrorKind.UNKNOWN  # type: ignore[union-attr]


def test_preserves_grants_non_update_and_non_replacing_statement() -> None:
    artifact = rendered()
    assert preserves_grants(change(artifact))
    live = observed(artifact, ownership=marker(artifact))
    safe = replace(artifact, statements=("ALTER SEMANTIC VIEW DB.SCHEMA.V SET COMMENT='x'",))
    assert preserves_grants(change(safe, Action.UPDATE, live=live))


def test_generic_apply_refuses_composite_artifact_program() -> None:
    artifact = replace(rendered(), key="eval:a", artifact_type="eval", generic_apply_safe=False)
    use_case, port, _, _ = runner()
    result = use_case.run(changeset(change(artifact)), state())
    assert result.outcomes[0].status is OutcomeStatus.FAILED
    assert "dedicated publication handler" in result.outcomes[0].error.message  # type: ignore[union-attr]
    assert port.scripts == []


def test_apply_honors_parallelism_within_a_dependency_wave() -> None:
    from threading import Barrier

    first = rendered("FIRST")
    second = rendered("SECOND")
    port = InMemorySnowflake()
    barrier = Barrier(2)
    original = port.execute_script

    def synchronized(statements):
        barrier.wait(timeout=2)
        return original(statements)

    port.execute_script = synchronized  # type: ignore[method-assign]
    use_case, _, _, _ = runner(port)
    result = use_case.run(
        changeset(change(first), change(second)),
        state(),
        ApplyOptions(parallelism=2),
    )
    assert result.success


def test_post_write_actor_metadata_never_queries_snowflake() -> None:
    artifact = rendered()
    port = InMemorySnowflake()
    port.current_role = lambda: (_ for _ in ()).throw(SnowflakePortError("metadata down"))  # type: ignore[method-assign]
    use_case, _, store, _ = runner(port)
    result = use_case.run(changeset(change(artifact)), state())
    assert result.success
    assert store.state is not None
    assert store.state.last_run is not None and store.state.last_run.actor == "TEST_ROLE"


def test_worker_exception_becomes_outcome_and_persists_sibling_success() -> None:
    first = rendered("FIRST")
    second = rendered("SECOND")
    ownership = marker(second)
    live_second = observed(second, ownership=ownership)
    port = InMemorySnowflake()
    port.markers[second.target.sql] = ownership

    def marker_with_failure(qualified_name):
        if qualified_name == second.target:
            raise RuntimeError("metadata failure")
        return port.markers.get(qualified_name.sql)

    port.describe_marker = marker_with_failure  # type: ignore[method-assign]
    use_case, _, store, _ = runner(port)

    result = use_case.run(
        changeset(
            change(first),
            change(second, Action.UPDATE, live=live_second),
        ),
        state(),
        ApplyOptions(parallelism=2),
    )

    assert not result.success
    assert result.state_written
    assert [outcome.status for outcome in result.outcomes] == [
        OutcomeStatus.APPLIED,
        OutcomeStatus.FAILED,
    ]
    assert store.state is not None
    assert first.key in store.state.applied
    assert second.key not in store.state.applied


def test_grant_preservation_ignores_grantor_drift() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    calls = 0

    def grants(object_type, qualified_name, routine_signature=()):
        nonlocal calls
        del object_type, qualified_name, routine_signature
        calls += 1
        return (
            GrantRow(
                "SELECT",
                "ROLE",
                "READER",
                "OLD_GRANTOR" if calls == 1 else "NEW_GRANTOR",
            ),
        )

    port.show_grants = grants  # type: ignore[method-assign]
    use_case, _, _, _ = runner(port)

    result = use_case.run(
        changeset(change(artifact, Action.UPDATE, live=live)),
        state(),
    )

    assert result.success
    assert result.outcomes[0].grants is GrantCheck.PRESERVED


def test_search_service_update_replays_and_verifies_explicit_grants() -> None:
    artifact = replace(
        rendered(),
        object_type="CORTEX SEARCH SERVICE",
        grant_preservation=GrantPreservation.REPLAY,
        statements=("CREATE OR REPLACE CORTEX SEARCH SERVICE DB.SCHEMA.V ON BODY AS SELECT BODY FROM DB.SCHEMA.T",),
    )
    ownership = marker(artifact)
    live = replace(observed(artifact, ownership=ownership), object_type="CORTEX SEARCH SERVICE")
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    grant = GrantRow("USAGE", "ROLE", "READER", grant_option=True)
    port.grants[artifact.target.sql] = (grant,)
    use_case, _, _, _ = runner(port)

    result = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())

    assert result.success
    assert port.scripts[0] == artifact.statements
    assert port.scripts[1] == ("GRANT USAGE ON CORTEX SEARCH SERVICE DB.SCHEMA.V TO ROLE READER WITH GRANT OPTION",)
    assert result.outcomes[0].grants is GrantCheck.PRESERVED


def test_search_service_replay_failure_retains_truthful_write_state() -> None:
    artifact = replace(
        rendered(),
        object_type="CORTEX SEARCH SERVICE",
        grant_preservation=GrantPreservation.REPLAY,
        statements=("CREATE OR REPLACE CORTEX SEARCH SERVICE DB.SCHEMA.V ON BODY AS SELECT BODY FROM DB.SCHEMA.T",),
    )
    ownership = marker(artifact)
    live = replace(observed(artifact, ownership=ownership), object_type="CORTEX SEARCH SERVICE")
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    port.grants[artifact.target.sql] = (GrantRow("USAGE", "ROLE", "READER"),)
    port.execute_results = [ExecResult(True), failed("grant denied", "28000")]
    use_case, _, store, _ = runner(port)

    result = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())

    assert not result.success
    assert result.outcomes[0].write_succeeded
    assert store.state is not None
    assert store.state.applied[artifact.key].outcome == "failed_after_write"


def test_agent_apply_uploads_spec_before_executing_version_program() -> None:
    artifact = replace(
        rendered(),
        object_type="AGENT",
        grant_preservation=GrantPreservation.NONE,
        upload_path="@DB.S.AGENT_SPECS/v/sha/agent_spec.yaml",
        upload_content=b"{}",
        statements=("CREATE AGENT IF NOT EXISTS DB.SCHEMA.V FROM @DB.S.AGENT_SPECS/v/sha/",),
    )
    use_case, port, _, _ = runner()
    result = use_case.run(changeset(change(artifact)), state())
    assert result.success
    assert port.uploads == [(artifact.upload_path, b"{}")]
    assert port.scripts == [artifact.statements]


def test_partial_statement_failure_records_truthful_recovery_state() -> None:
    artifact = rendered()
    port = InMemorySnowflake()
    port.execute_results = [ExecResult(False, ("query-1",), error=ExecutionError("later failed"))]
    use_case, _, store, _ = runner(port)
    result = use_case.run(changeset(change(artifact)), state())
    assert not result.success
    assert result.outcomes[0].write_succeeded
    assert store.state is not None
    assert store.state.applied[artifact.key].outcome == "failed_after_write"


def test_expected_marker_must_be_visible_after_multi_statement_publish() -> None:
    base = rendered()
    artifact = replace(
        base,
        expected_marker=OwnershipMarker("b" * 64, base.fingerprint),
    )
    use_case, _, store, _ = runner()
    result = use_case.run(changeset(change(artifact)), state())
    assert not result.success
    assert result.outcomes[0].write_succeeded
    assert result.outcomes[0].error.code == "SST-APL012"  # type: ignore[union-attr]
    assert store.state is not None
    assert store.state.applied[artifact.key].outcome == "failed_after_write"


def test_an_error_reading_the_marker_back_after_create_keeps_ownership() -> None:
    base = rendered()
    published = manifest({base.key: base})
    ownership = OwnershipMarker(published.manifest_id, base.fingerprint)
    artifact = replace(base, expected_marker=ownership)
    port = InMemorySnowflake()
    # The CREATE succeeds; the SHOW that reads its marker back then fails.
    port.marker_error = SnowflakePortError("connection reset", sqlstate="08001")
    use_case, _, store, _ = runner(port)

    result = use_case.run(replace(changeset(change(artifact)), manifest_id=published.manifest_id), state())

    assert port.scripts == [artifact.statements]
    assert store.state is not None
    port.marker_error = None
    port.rows = (ShowRow("V", "DB", "SCHEMA", "OWNER", "now", f"published {ownership.text}"),)
    replanned = PlanArtifacts(port).run({artifact.key: artifact}, published, store.state, target(), fetched_at="later")
    # The view is SST's: the next plan must not call it unmanaged.
    assert [item.code for item in replanned.diagnostics] == []
    assert replanned.changes[0].action is Action.NOOP
    assert not result.success
    assert result.outcomes[0].status is OutcomeStatus.FAILED and result.outcomes[0].write_succeeded
    assert store.state.applied[artifact.key].outcome == FAILED_AFTER_WRITE


def test_an_error_rechecking_grants_after_update_records_the_write() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    reads = 0

    def grants(object_type, qualified_name, routine_signature=()):
        nonlocal reads
        del object_type, qualified_name, routine_signature
        reads += 1
        if reads == 1:
            return (GrantRow("SELECT", "ROLE", "R"),)
        raise RuntimeError("unexpected SHOW GRANTS row")

    port.show_grants = grants  # type: ignore[method-assign]
    use_case, _, store, _ = runner(port)

    result = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())

    assert port.scripts == [artifact.statements]
    assert result.outcomes[0].status is OutcomeStatus.FAILED
    assert result.outcomes[0].write_succeeded
    assert result.outcomes[0].error.message == "unexpected SHOW GRANTS row"  # type: ignore[union-attr]
    assert store.state is not None
    assert store.state.applied[artifact.key].outcome == FAILED_AFTER_WRITE
    assert store.state.applied[artifact.key].fingerprint == artifact.fingerprint


def test_database_role_grant_replay_uses_snowflake_spelling() -> None:
    artifact = replace(
        rendered(),
        object_type="CORTEX SEARCH SERVICE",
        grant_preservation=GrantPreservation.REPLAY,
        statements=("CREATE OR REPLACE CORTEX SEARCH SERVICE DB.SCHEMA.V ON BODY AS SELECT BODY FROM DB.SCHEMA.T",),
    )
    ownership = marker(artifact)
    live = replace(observed(artifact, ownership=ownership), object_type="CORTEX SEARCH SERVICE")
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    port.grants[artifact.target.sql] = (GrantRow("USAGE", "DATABASE_ROLE", "DB.READER"),)
    use_case, _, _, _ = runner(port)
    result = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert result.success
    assert port.scripts[1] == ("GRANT USAGE ON CORTEX SEARCH SERVICE DB.SCHEMA.V TO DATABASE ROLE DB.READER",)


def test_grant_replay_quotes_special_role_names() -> None:
    artifact = replace(
        rendered(),
        object_type="CORTEX SEARCH SERVICE",
        grant_preservation=GrantPreservation.REPLAY,
        statements=("CREATE OR REPLACE CORTEX SEARCH SERVICE DB.SCHEMA.V ON BODY AS SELECT BODY FROM DB.SCHEMA.T",),
    )
    ownership = marker(artifact)
    live = replace(observed(artifact, ownership=ownership), object_type="CORTEX SEARCH SERVICE")
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = ownership
    port.grants[artifact.target.sql] = (GrantRow("USAGE", "ROLE", "Mixed Role"),)
    use_case, _, _, _ = runner(port)
    result = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert result.success
    assert port.scripts[1] == ('GRANT USAGE ON CORTEX SEARCH SERVICE DB.SCHEMA.V TO ROLE "Mixed Role"',)


def test_apply_stop_all_processes_successes_before_first_failure() -> None:
    first = rendered("FIRST")
    second = rendered("SECOND")
    third = rendered("THIRD")
    port = InMemorySnowflake()
    port.execute_results = [ExecResult(True), failed("syntax error", "42000")]
    use_case, _, _, _ = runner(port)
    result = use_case.run(
        changeset(change(first), change(second), change(third)),
        state(),
        ApplyOptions(on_failure=FailurePolicy.STOP_ALL),
    )
    assert [outcome.status for outcome in result.outcomes] == [
        OutcomeStatus.APPLIED,
        OutcomeStatus.FAILED,
        OutcomeStatus.SKIPPED,
    ]

    port = InMemorySnowflake()
    use_case, _, _, _ = runner(port)
    successful = use_case.run(
        changeset(change(first), change(second)),
        state(),
        ApplyOptions(on_failure=FailurePolicy.STOP_ALL),
    )
    assert successful.success
    assert [outcome.status for outcome in successful.outcomes] == [OutcomeStatus.APPLIED, OutcomeStatus.APPLIED]


def test_apply_lifecycle_handler_merges_resources_and_dispatches_diagnostic() -> None:
    artifact = replace(
        rendered(),
        artifact_type="virtual",
        physical_resources=(("TABLE", rendered("PHYSICAL").target),),
    )
    handler = _LifecycleHandler()
    use_case, _, store, _ = runner()
    use_case._lifecycle_handlers["virtual"] = handler
    value = change(artifact)
    value = replace(value, artifact_type="virtual")
    result = use_case.run(changeset(value), state())
    assert result.success
    assert handler.applied
    assert handler.merged
    assert store.state is not None
    assert [
        (item.object_type, item.qualified_name) for item in store.state.applied[artifact.key].applied_resources
    ] == [("TABLE", "DB.SCHEMA.MERGED")]

    for code in ("SST-APL016", "SST-APL022"):
        outcome = replace(
            handler.apply(value, ApplyOptions()),
            status=OutcomeStatus.FAILED,
            error=ClassifiedError(code, "detail", ErrorKind.UNKNOWN),
        )
        diagnostic = _outcome_diagnostic(value, outcome)
        assert diagnostic.code == code

    authored = replace(
        value,
        diagnostics=DiagnosticBag((D("SST-APL028", value=value.key, found="old", expected="new"),)),
    )
    outcome = replace(
        handler.apply(authored, ApplyOptions()),
        status=OutcomeStatus.FAILED,
        error=ClassifiedError("SST-APL028", "detail", ErrorKind.UNKNOWN),
    )
    assert _outcome_diagnostic(authored, outcome) is authored.diagnostics[0]
    assert _outcome_diagnostic(value, outcome).code == "SST-INT902"


def test_apply_upload_failure_and_empty_grant_replay() -> None:
    artifact = replace(rendered(), upload_path="@DB.S.FILE", upload_content=b"payload")
    port = InMemorySnowflake()

    def fail_upload(stage_path, content):
        del stage_path, content
        raise SnowflakePortError("timeout", sqlstate="08001")

    port.upload = fail_upload  # type: ignore[method-assign]
    use_case, _, _, _ = runner(port)
    result = use_case.run(changeset(change(artifact)), state())
    assert result.outcomes[0].error.kind is ErrorKind.TRANSIENT  # type: ignore[union-attr]

    replay = replace(
        artifact,
        object_type="CORTEX SEARCH SERVICE",
        grant_preservation=GrantPreservation.REPLAY,
    )
    ownership = marker(replay)
    live = replace(observed(replay, ownership=ownership), object_type="CORTEX SEARCH SERVICE")
    port = InMemorySnowflake()
    port.markers[replay.target.sql] = ownership
    use_case, _, _, _ = runner(port)
    empty = use_case.run(changeset(change(replay, Action.UPDATE, live=live)), state())
    assert empty.success
    assert len(port.scripts) == 1


def test_apply_exhausts_retry_and_classifies_marker_lookup_exception() -> None:
    artifact = rendered()
    port = InMemorySnowflake()
    port.execute_results = [failed("timeout", "08001"), failed("timeout", "08001")]
    use_case, _, _, _ = runner(port)
    exhausted = use_case.run(
        changeset(change(artifact)),
        state(),
        ApplyOptions(retry=RetryPolicy(2, (1,))),
    )
    assert not exhausted.success
    assert exhausted.outcomes[0].attempts == 2

    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    port = InMemorySnowflake()
    port.marker_error = SnowflakePortError("denied", sqlstate="28000")
    use_case, _, _, _ = runner(port)
    failed_lookup = use_case.run(changeset(change(artifact, Action.UPDATE, live=live)), state())
    assert failed_lookup.outcomes[0].error.kind is ErrorKind.PRIVILEGE  # type: ignore[union-attr]


def test_apply_prune_non_executable_and_observed_marker_state_paths() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    live = observed(artifact, ownership=ownership)
    non_executable = replace(change(artifact, Action.PRUNE, live=live), prune_executable=False)
    use_case, _, store, _ = runner()
    from snowflake_semantic_tools.domain.state import AppliedEntry

    recorded = AppliedEntry(
        artifact.fingerprint, artifact.target.sql, "now", "prior", "applied", artifact.fingerprint, "o" * 64
    )
    prior = replace(state(), applied=MappingProxyType({artifact.key: recorded}))
    skipped = use_case.run(changeset(non_executable), prior, ApplyOptions(allow_prune=True))
    assert skipped.outcomes[0].status is OutcomeStatus.SKIPPED
    # Nothing executes, but the retained entry moves to this manifest so the plan reconciles.
    assert skipped.state_written and store.state is not None
    assert store.state.applied[artifact.key] == replace(recorded, manifest_id="m" * 64)

    noop = replace(change(artifact), action=Action.NOOP, observed=live)
    use_case, _, store, _ = runner()
    observed_result = use_case.run(changeset(noop), state())
    assert not observed_result.state_written
    assert store.state is None

    blocked = replace(change(artifact), action=Action.BLOCKED, observed=live)
    use_case, _, store, _ = runner()
    retained = use_case.run(changeset(blocked), state(), ApplyOptions(on_failure=FailurePolicy.CONTINUE))
    assert store.state is None
    assert not retained.state_written


def test_finish_state_skips_markerless_observation_and_unrendered_outcome() -> None:
    artifact = rendered()
    live_without_marker = observed(artifact)
    malformed = replace(change(artifact), rendered=None, observed=live_without_marker)
    use_case, _, _, _ = runner()
    state_value = use_case._finish_state(
        changeset(malformed),
        state(),
        (
            ApplyOutcome(
                malformed.key,
                malformed.action,
                OutcomeStatus.APPLIED,
                1,
                0,
                "",
                write_succeeded=True,
            ),
        ),
        "run",
        "start",
    )
    assert state_value.applied == {}


def test_finish_state_prunes_applied_entry_and_preserves_prior_failed_entry() -> None:
    artifact = rendered()
    from snowflake_semantic_tools.domain.state import AppliedEntry

    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "before",
        "prior",
        "applied",
        artifact.fingerprint,
        "a" * 64,
    )
    previous = replace(state(), applied=MappingProxyType({artifact.key: entry}))
    prune = change(artifact, Action.PRUNE)
    use_case, _, _, _ = runner()
    pruned = use_case._finish_state(
        changeset(prune),
        previous,
        (ApplyOutcome(prune.key, Action.PRUNE, OutcomeStatus.APPLIED, 1, 0, "", write_succeeded=True),),
        "run",
        "start",
    )
    assert artifact.key not in pruned.applied

    failed_without_write = change(artifact, Action.UPDATE, live=observed(artifact, ownership=marker(artifact)))
    retained = use_case._finish_state(
        changeset(failed_without_write),
        previous,
        (
            ApplyOutcome(
                failed_without_write.key,
                Action.UPDATE,
                OutcomeStatus.FAILED,
                1,
                0,
                artifact.ddl,
                ClassifiedError("SST-SNO001", "failed", ErrorKind.UNKNOWN),
            ),
        ),
        "run",
        "start",
    )
    assert retained.applied[artifact.key] == entry


def test_a_report_only_prune_without_an_entry_records_the_object_it_observed() -> None:
    artifact = rendered()
    ownership = marker(artifact)
    report_only = replace(
        change(artifact, Action.PRUNE, live=observed(artifact, ownership=ownership)), prune_executable=False
    )
    use_case, port, store, _ = runner()

    result = use_case.run(changeset(report_only), state(), ApplyOptions(allow_prune=True))

    assert result.state_written and port.scripts == []
    assert store.state is not None
    recorded = store.state.applied[artifact.key]
    assert (recorded.outcome, recorded.manifest_id) == ("observed", ownership.manifest_id)


def test_outcomes_that_do_not_account_for_every_change_report_apl900() -> None:
    artifact = rendered()
    use_case, port, _, _ = runner()

    # The waves key changes by artifact key, so a repeated key runs once.
    result = use_case.run(changeset(change(artifact), change(artifact)), state())

    assert len(result.outcomes) == 1 and port.scripts == [artifact.statements]
    assert [item.code for item in result.diagnostics] == ["SST-APL900"]
    assert not result.success


def test_a_retry_policy_without_attempts_fails_the_change_without_running_it() -> None:
    artifact = rendered()
    use_case, port, store, clock = runner()

    result = use_case.run(changeset(change(artifact)), state(), ApplyOptions(retry=RetryPolicy(0, ())))

    outcome = result.outcomes[0]
    assert (outcome.status, outcome.attempts, outcome.write_succeeded) == (OutcomeStatus.FAILED, 0, False)
    assert outcome.error is not None and outcome.error.code == "SST-SNO001"
    assert port.scripts == [] and clock.sleeps == []
    assert store.state is not None and artifact.key not in store.state.applied


class _LifecycleHandler:
    artifact_type = "virtual"

    def __init__(self) -> None:
        self.applied = False
        self.merged = False

    def apply(self, change, options):
        del options
        self.applied = True
        return ApplyOutcome(
            change.key,
            change.action,
            OutcomeStatus.APPLIED,
            1,
            1,
            change.rendered.ddl,
            write_succeeded=True,
        )

    def merge_physical_resources(self, current, previous):
        del current, previous
        self.merged = True
        return (("TABLE", "DB.SCHEMA.MERGED"),)
