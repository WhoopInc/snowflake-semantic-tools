"""The drift digest: which properties each kind digests, how two digests compare, and the plan rule."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeReason,
    DesiredMetadata,
    ObservedArtifact,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    ShowRow,
    StatementPlan,
    drift_properties,
    drifted_properties,
    live_digest,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, manifest_of, plan
from tests.helpers.sql_values import statement

SPEC = '{"models": {"orchestration": "auto"}}'


def test_each_kind_digests_the_properties_its_marker_does_not_prove() -> None:
    shown = {"is_secure": " n ", "target_lag": "1  day", "warehouse": "wh"}
    described = {"execute as": "caller", "body": "BEGIN\n  RETURN 1;\nEND;"}
    assert drift_properties("agent", {}, newest_spec="a" * 64) == {"specification": "a" * 64}
    assert drift_properties("CORTEX  SEARCH SERVICE", shown) == {"target_lag": "1 DAY", "warehouse": "WH"}
    assert drift_properties("FUNCTION", shown) == {"secure": "N"}
    assert drift_properties("PROCEDURE", shown, described=described) == {
        "secure": "N",
        "execute_as": "CALLER",
        "body": "BEGIN\n  RETURN 1;\nEND;",
    }
    # A basic check reads no DESCRIBE, so only what SHOW lists is digested.
    assert drift_properties("PROCEDURE", {}) == {"secure": None, "execute_as": None, "body": None}
    assert drift_properties("SEMANTIC VIEW", {}, definition="create  view V\n (x);\n") == {
        "definition": "create view V (x)"
    }
    assert drift_properties("SEMANTIC VIEW", {}) == {"definition": None}
    assert drift_properties("STAGE", shown) == {}


def test_a_digest_holds_one_short_hash_per_property_read_and_none_when_nothing_was() -> None:
    digest = live_digest({"secure": "N", "body": None, "execute_as": "CALLER"})
    assert digest is not None and [pair.split("=")[0] for pair in digest.split(";")] == ["execute_as", "secure"]
    assert all(len(pair.split("=")[1]) == 16 for pair in digest.split(";"))
    assert live_digest({"body": None}) is None and live_digest({}) is None


def test_only_the_properties_both_digests_hold_are_compared() -> None:
    full = live_digest({"secure": "N", "execute_as": "CALLER", "body": "x"})
    basic = live_digest({"secure": "N"})
    owner = live_digest({"secure": "N", "execute_as": "OWNER", "body": "x"})
    assert drifted_properties(full, owner) == ("execute_as",)
    assert drifted_properties(full, basic) == () and drifted_properties(basic, owner) == ()
    assert drifted_properties(None, owner) == () and drifted_properties(full, None) == ()
    # A part that is not a pair is unread, so it never compares as drift.
    assert drifted_properties("secure=ab;garbage;body=cd", "secure=ab;body=ce") == ("body",)


def _agent() -> RenderedArtifact:
    artifact = RenderedArtifact.create(
        key="agent:sales",
        artifact_type="agent",
        target=QualifiedName.parse("DB.S.SALES"),
        ddl=SPEC,
        shape=PublishShape("AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE),
        statements=StatementPlan(default=(statement("create"),), update=(statement("add version"),)),
        metadata=DesiredMetadata(alias="promoted", spec_fingerprint=spec_fingerprint(SPEC)),
    )
    return replace(artifact, fingerprint="1" * 64)


def _live_agent(artifact: RenderedArtifact, marker: OwnershipMarker, newest_spec: str) -> ObservedArtifact:
    return ObservedArtifact(
        artifact.key,
        "SALES",
        artifact.target,
        "AGENT",
        "OWNER",
        "now",
        marker.text,
        marker,
        (),
        has_live_version=True,
        newest_version="VERSION$4",
        newest_spec=newest_spec,
        # The alias is on an older version, which version drift alone would put back.
        alias_versions=(("PROMOTED", "VERSION$3"),),
        live_digest=live_digest({"specification": newest_spec}),
    )


def _recorded(artifact: RenderedArtifact, manifest_id: str) -> AppliedEntry:
    return replace(entry(artifact, manifest_id), live_digest=live_digest({"specification": spec_fingerprint(SPEC)}))


def test_a_hand_committed_agent_specification_under_an_unchanged_marker_blocks_before_version_drift() -> None:
    artifact = _agent()
    planned_manifest = manifest_of(artifact)
    marker = OwnershipMarker(planned_manifest.manifest_id, artifact.fingerprint)
    observed = _live_agent(artifact, marker, spec_fingerprint('{"instructions": "pirate"}'))
    applied = {artifact.key: _recorded(artifact, planned_manifest.manifest_id)}
    planned = plan((artifact,), observed=(observed,), applied=applied, manifest=planned_manifest)
    assert planned.changes[0].action is Action.BLOCKED
    assert [item.code for item in coded(planned.diagnostics, "SST-PLN014")] == ["SST-PLN014"]
    assert coded(planned.diagnostics, "SST-PLN102") == []


def test_an_alias_moved_onto_a_version_holding_sst_s_specification_is_version_drift_not_drift() -> None:
    artifact = _agent()
    planned_manifest = manifest_of(artifact)
    marker = OwnershipMarker(planned_manifest.manifest_id, artifact.fingerprint)
    observed = _live_agent(artifact, marker, spec_fingerprint(SPEC))
    applied = {artifact.key: _recorded(artifact, planned_manifest.manifest_id)}
    planned = plan((artifact,), observed=(observed,), applied=applied, manifest=planned_manifest)
    assert (planned.changes[0].action, planned.changes[0].reason) == (Action.UPDATE, ChangeReason.VERSION_DRIFT)
    assert coded(planned.diagnostics, "SST-PLN014") == []


def test_a_drifted_property_blocks_an_update_that_would_replace_the_hand_edit() -> None:
    artifact = _agent()
    planned_manifest = manifest_of(artifact)
    marker = OwnershipMarker(planned_manifest.manifest_id, artifact.fingerprint)
    observed = _live_agent(artifact, marker, spec_fingerprint("{}"))
    # The project changed too, so without the digest the plan would update over the edit.
    changed = replace(artifact, fingerprint="2" * 64)
    applied = {artifact.key: _recorded(artifact, planned_manifest.manifest_id)}
    planned = plan((changed,), observed=(observed,), applied=applied, manifest=planned_manifest)
    assert planned.changes[0].action is Action.BLOCKED


def test_an_entry_round_trips_its_digest_and_an_older_entry_reads_without_one() -> None:
    recorded = replace(entry(_agent(), "a" * 64), live_digest="specification=0123456789abcdef")
    assert AppliedEntry.from_dict(recorded.as_dict()) == recorded
    older = entry(_agent(), "a" * 64).as_dict()
    assert "live_digest" not in older and AppliedEntry.from_dict(older).live_digest is None


def test_a_show_row_gives_the_columns_a_digest_reads_that_it_lists() -> None:
    row = ShowRow("P", "DB", "S", "OWNER", "now", object_type="PROCEDURE", is_secure="N")
    assert row.shown == {"is_secure": "N"}
    assert ShowRow("V", "DB", "S", "OWNER", "now").shown == {}
