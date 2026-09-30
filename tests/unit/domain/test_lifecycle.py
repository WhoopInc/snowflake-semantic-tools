"""Lifecycle boundary values are immutable and explicit."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    ApplyResult,
    Change,
    ChangeReason,
    ChangeSet,
    ClassifiedError,
    DesiredMetadata,
    ErrorKind,
    ExecResult,
    ExecutionError,
    GrantCheck,
    GrantRow,
    ObservedArtifact,
    OutcomeStatus,
    OwnershipMarker,
    ProbeKind,
    PublishShape,
    QueryResult,
    RenderedArtifact,
    RetryPolicy,
    ShowRow,
    SmokeProbe,
    SnowflakeObservation,
    StatementPlan,
    extract_marker,
)


def target() -> TargetIdentity:
    return TargetIdentity("dev", "acct", Identifier.parse("db"), Identifier.parse("sch"))


def rendered(name: str = "VIEW") -> RenderedArtifact:
    return RenderedArtifact.create(
        key=f"semantic_view:{name.casefold()}",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts("db", "sch", name),
        ddl="create semantic view x  \n",
        smoke=(SmokeProbe("view:x", ProbeKind.VIEW, "select 1"),),
    )


def test_ownership_marker_validation_and_extraction() -> None:
    marker = OwnershipMarker("a" * 64, "B" * 64)
    assert marker.text == f"[sst:{'a' * 64}:{'b' * 64}]"
    assert extract_marker(None) is None
    assert extract_marker("ordinary comment") is None
    assert extract_marker(f"old [sst:{'c' * 64}:{'d' * 64}] {marker.text}") == marker
    with pytest.raises(ValueError, match="manifest_id"):
        OwnershipMarker("short", "b" * 64)
    with pytest.raises(ValueError, match="fingerprint"):
        OwnershipMarker("a" * 64, "not-hex" * 10)


def test_grants_show_rows_and_observations_keep_three_states() -> None:
    explicit = GrantRow("SELECT", "role", "reader")
    assert explicit.identity == ("SELECT", "ROLE", "READER", False)
    ownership = GrantRow("OWNERSHIP", "ROLE", "owner")
    inherited = GrantRow("SELECT", "DATABASE_ROLE", "db.reader")
    assert explicit.is_explicit
    assert not ownership.is_explicit and inherited.is_explicit
    row = ShowRow("v", "db", "sch", "owner", "now", "comment")
    assert row.qualified_name.sql == "DB.SCH.V"
    observed = SnowflakeObservation(
        MappingProxyType(
            {
                "semantic_view:v": ObservedArtifact(
                    "semantic_view:v",
                    "V",
                    row.qualified_name,
                    "SEMANTIC VIEW",
                    "owner",
                    "now",
                    None,
                    None,
                    (explicit, ownership, inherited),
                )
            }
        ),
        "now",
    )
    assert observed.artifacts["semantic_view:v"].explicit_grants == (explicit, inherited)


def test_rendered_artifact_canonicalizes_and_accepts_explicit_statements() -> None:
    artifact = rendered()
    assert artifact.ddl == "create semantic view x\n"
    assert artifact.content == artifact.ddl
    assert artifact.object_type == "SEMANTIC VIEW"
    assert artifact.render_dialect == "ddl"
    assert artifact.statements == ("create semantic view x",)
    assert len(artifact.fingerprint) == 64
    explicit = RenderedArtifact.create(
        key="semantic_view:x",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts("db", "sch", "x"),
        ddl="payload",
        shape=PublishShape("AGENT", render_dialect="json"),
        statements=StatementPlan(default=("one", "two")),
        depends_on=("semantic_view:y",),
    )
    assert explicit.statements == ("one", "two")
    assert explicit.object_type == "AGENT"
    assert explicit.render_dialect == "json"

    observed = ObservedArtifact(
        "agent:x",
        "X",
        QualifiedName.from_parts("db", "sch", "x"),
        "AGENT",
        "owner",
        "now",
        None,
        None,
        aliases=("KEEP", "Mixed Alias"),
        tags=("DB.S.KEEP", "broken tag"),
    )
    metadata = RenderedArtifact.create(
        key="agent:x",
        artifact_type="agent",
        target=observed.qualified_name,
        ddl="{}",
        shape=PublishShape("AGENT"),
        statements=StatementPlan(update=("ALTER AGENT ADD VERSION",)),
        metadata=DesiredMetadata(alias="KEEP", tags=("DB.S.KEEP",)),
    ).for_action(Action.UPDATE, observed)
    assert not any("KEEP UNSET ALIAS" in statement for statement in metadata.statements)
    assert any('"Mixed Alias" UNSET ALIAS' in statement for statement in metadata.statements)
    assert any('UNSET TAG "broken tag"' in statement for statement in metadata.statements)

    ordinary = rendered()
    assert ordinary.for_action(Action.UPDATE, None) is ordinary
    non_agent = replace(ordinary, update_statements=("update",))
    non_agent_observed = ObservedArtifact(
        non_agent.key,
        "V",
        non_agent.target,
        "SEMANTIC VIEW",
        "owner",
        "now",
        None,
        None,
    )
    assert non_agent.for_action(Action.UPDATE, non_agent_observed).statements == ("update",)


def test_rendered_artifact_selects_create_live_update_and_safe_metadata_statements() -> None:
    agent = RenderedArtifact.create(
        key="agent:a",
        artifact_type="agent",
        target=QualifiedName.from_parts("db", "sch", "a"),
        ddl="{}",
        shape=PublishShape("AGENT"),
        statements=StatementPlan(create=("create",), update=("update",), update_live=("update live",)),
        metadata=DesiredMetadata(tags=("DB.S.KEEP",)),
    )
    observed = ObservedArtifact(
        agent.key,
        "A",
        agent.target,
        "AGENT",
        "owner",
        "now",
        None,
        None,
        has_live_version=True,
        aliases=("SAFE",),
        tags=("DB.S.STALE",),
    )
    assert agent.for_action(Action.CREATE, None).statements == ("create",)
    live = agent.for_action(Action.UPDATE, observed)
    assert live.statements == (
        "update live",
        "ALTER AGENT DB.SCH.A MODIFY VERSION SAFE UNSET ALIAS",
        "ALTER AGENT DB.SCH.A UNSET TAG DB.S.STALE",
    )
    no_stale_tags = replace(observed, tags=("DB.S.KEEP",))
    assert agent.for_action(Action.UPDATE, no_stale_tags).statements == (
        "update live",
        "ALTER AGENT DB.SCH.A MODIFY VERSION SAFE UNSET ALIAS",
    )

    quoted = replace(agent, desired_alias=None, desired_tags=())
    malformed = replace(observed, aliases=("1 bad",), tags=("DB..TAG",))
    statements = quoted.for_action(Action.UPDATE, malformed).statements
    assert 'MODIFY VERSION "1 bad" UNSET ALIAS' in statements[1]
    assert 'UNSET TAG DB."".TAG' in statements[2]


def test_changeset_partitions_writes_and_blocked() -> None:
    artifact = rendered()
    changes = tuple(
        Change(
            f"key:{action.value}",
            "semantic_view",
            action,
            ChangeReason.UNCHANGED,
            artifact if action is not Action.PRUNE else None,
            None,
            (),
            100,
        )
        for action in Action
    )
    changeset = ChangeSet("m", target(), changes, DiagnosticBag(), "now")
    assert tuple(change.action for change in changeset.writes) == (
        Action.CREATE,
        Action.UPDATE,
        Action.PRUNE,
    )
    assert changeset.blocked[0].action is Action.BLOCKED
    assert changeset.report_only == ()
    listed_changes = tuple(
        replace(change, prune_executable=False) if change.action is Action.PRUNE else change for change in changes
    )
    listed = ChangeSet("m", target(), listed_changes, DiagnosticBag(), "now")
    assert tuple(change.action for change in listed.writes) == (Action.CREATE, Action.UPDATE)
    assert tuple(change.action for change in listed.report_only) == (Action.PRUNE,)


def test_retry_policy_apply_results_and_transport_values() -> None:
    retry = RetryPolicy()
    assert retry.delay_after(1) == 1000
    assert retry.delay_after(2) == 2000
    with pytest.raises(ValueError):
        retry.delay_after(0)
    with pytest.raises(ValueError):
        retry.delay_after(3)
    short_schedule = RetryPolicy(4, (5,))
    assert short_schedule.delay_after(3) == 5
    assert ApplyOptions().retry == retry

    failure = ClassifiedError("SST-APL001", "bad", ErrorKind.SYNTAX)
    failed = ApplyOutcome("x", Action.UPDATE, OutcomeStatus.FAILED, 1, 3, "ddl", failure)
    applied = ApplyOutcome("y", Action.CREATE, OutcomeStatus.APPLIED, 1, 2, "ddl", grants=GrantCheck.PRESERVED)
    good = ApplyResult((applied,), DiagnosticBag(), "run", "start", "finish", True)
    bad = ApplyResult((failed,), DiagnosticBag(), "run", "start", "finish", True)
    diagnostic_bad = ApplyResult(
        (applied,), DiagnosticBag((D("SST-APL003", artifact="x", count=1),)), "run", "s", "f", False
    )
    assert good.success and not bad.success and not diagnostic_bad.success
    assert QueryResult().rows == ()
    assert ExecResult(True).ok
    assert ExecResult(True, rows_affected=0).rows_affected == 0
    transport_error = ExecutionError("failed", "42000", 1)
    assert ExecResult(False, error=transport_error).error == transport_error


def test_a_shown_name_that_cannot_be_unquoted_is_the_exact_quoted_name() -> None:
    assert ShowRow("v", "db", "sch", "owner", "now").qualified_name.sql == "DB.SCH.V"
    operator = ShowRow("!=", "DB", "SCH", "", "now").qualified_name
    assert (operator.name.value, operator.name.quoted, operator.sql) == ("!=", True, 'DB.SCH."!="')
    assert ShowRow("my view", "Mixed Db", "SCH", "", "now").qualified_name.sql == '"Mixed Db".SCH."my view"'
    assert Identifier.shown("Sales") == Identifier("SALES")
    with pytest.raises(ValueError, match="empty identifier"):
        Identifier.shown("")
