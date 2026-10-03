"""The states `sst diff` reads: a live target by its state table and markers, and a saved plan."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.diff import live_states, manifest_states, plan_states
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.plan.diff import ArtifactState
from snowflake_semantic_tools.domain.state import APPLIED, DEACTIVATED, AppliedEntry
from tests.helpers.snowflake_fake import FakeSnowflake

TABLE = QualifiedName.from_parts("DB", "S", "SST_STATE")


def _entry(target: str, fingerprint: str, *, outcome: str = APPLIED) -> AppliedEntry:
    return AppliedEntry(fingerprint, target, "now", "run", outcome, fingerprint, "a" * 64)


def test_live_states_read_markers_and_trust_state_for_composites() -> None:
    port = FakeSnowflake(
        state={
            "semantic_view:kept": _entry("DB.S.KEPT", "1" * 64),
            "semantic_view:unmarked": _entry("DB.S.UNMARKED", "2" * 64),
            "skill:bundle": _entry("DB.S.BUNDLE", "3" * 64),
            "agent:retired": _entry("DB.S.RETIRED", "4" * 64, outcome=DEACTIVATED),
        },
        markers={"DB.S.KEPT": OwnershipMarker("a" * 64, "5" * 64)},
    )
    held, problems = live_states(port, TABLE, "dev")
    assert problems == ()
    assert held == {
        "semantic_view:kept": ArtifactState("5" * 64, "DB.S.KEPT", "a" * 64),
        "skill:bundle": ArtifactState("3" * 64, "DB.S.BUNDLE", "a" * 64),
    }


def test_live_states_report_an_unreadable_state_table(monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake()
    monkeypatch.setattr(port, "read_state", lambda table, target: None)
    held, [problem] = live_states(port, TABLE, "dev")
    assert held is None
    assert problem.code == "SST-MAN022"


def test_the_manifest_state_is_what_it_renders() -> None:
    from snowflake_semantic_tools.domain.state.manifest import ArtifactEntry, ImpactIndex, Manifest

    entry = ArtifactEntry("semantic_view", "v", "1" * 64, (), (), (), "DB.S.V", 10)
    manifest = Manifest(1, "m" * 64, {}, {}, {}, {"semantic_view:v": entry}, {}, {}, {}, ImpactIndex(), {})
    assert manifest_states(manifest) == {"semantic_view:v": ArtifactState("1" * 64, "DB.S.V", "m" * 64)}


def test_a_saved_plan_leaves_out_what_it_prunes() -> None:
    from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
    from snowflake_semantic_tools.domain.state import SavedPlan
    from snowflake_semantic_tools.domain.state.saved_plan import SavedChange

    def change(key: str, action: str, fingerprint: str | None) -> SavedChange:
        return SavedChange(key, "semantic_view", action, "r", "DB.S.X", fingerprint, None, (), (), 1)

    target = TargetIdentity("dev", "a", QualifiedName.parse("DB.S.X").database, QualifiedName.parse("DB.S.X").schema)
    plan = SavedPlan(
        1,
        "p",
        "m",
        target,
        "now",
        "o",
        (change("v:a", "create", "1"), change("v:b", "prune", None), change("v:c", "noop", None)),
    )
    assert plan_states(plan) == {"v:a": ArtifactState("1", "DB.S.X", "m")}
