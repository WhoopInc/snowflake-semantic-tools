from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore, PlanFileStore, StateFileStore
from snowflake_semantic_tools.domain.state import SavedPlan
from tests.unit.app.helpers import change, changeset, manifest, rendered, state


def test_manifest_state_and_plan_stores_round_trip_atomically(tmp_path: Path) -> None:
    artifact = rendered()
    manifest_value = manifest({artifact.key: artifact})
    manifest_store = ManifestFileStore(tmp_path / "target/sst/manifest.json")
    assert manifest_store.read() is None
    manifest_store.write(manifest_value)
    assert manifest_store.read() == manifest_value

    state_store = StateFileStore(tmp_path / "target/sst/state.json")
    state_store.write_local(state())
    assert state_store.read_local() == state()

    saved = SavedPlan.from_changeset(changeset(change(artifact)))
    plan_store = PlanFileStore(tmp_path / "target/sst/plan.json")
    plan_store.write(saved)
    assert plan_store.read() == saved
    assert not tuple((tmp_path / "target/sst").glob(".*.tmp*"))


def test_plan_store_detects_tampering_and_shape_errors(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    store = PlanFileStore(path)
    saved = SavedPlan.from_changeset(changeset(change(rendered())))
    store.write(saved)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["manifest_id"] = "tampered"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="recomputed"):
        store.read()


def test_state_lock_enforces_exclusivity_and_stale_break(tmp_path: Path) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store = StateFileStore(tmp_path / "state.json", now=lambda: now)
    assert store.acquire_lock("one", break_stale=False) == (True, None, False)
    assert store.acquire_lock("two", break_stale=False) == (False, "one", False)
    store.release_lock("one")

    stale = now - timedelta(hours=1)
    lock_path = (tmp_path / "state.json").with_suffix(".json.lock")
    lock_path.write_text(json.dumps({"run_id": "old", "created_at": stale.isoformat()}), encoding="utf-8")
    assert store.acquire_lock("new", break_stale=False) == (False, "old", False)
    assert store.acquire_lock("new", break_stale=True) == (True, "old", True)
    store.release_lock("old")
    assert store.acquire_lock("other", break_stale=False) == (False, "new", False)
    store.release_lock("new")
