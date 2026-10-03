from __future__ import annotations

import contextlib
import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore, PlanFileStore, StateFileStore
from snowflake_semantic_tools.domain.state import SavedPlan
from tests.helpers.artifact_builders import change, changeset, manifest, rendered, state


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
    now = datetime(2026, 1, 1, tzinfo=UTC)
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


NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _stale_lock(tmp_path: Path) -> Path:
    """Leave a lock file an hour older than its time to live, held by run `old`."""
    lock_path = tmp_path / "state.json.lock"
    lock_path.write_text(json.dumps({"run_id": "old", "created_at": (NOW - timedelta(hours=1)).isoformat()}))
    return lock_path


class _Breaker(StateFileStore):
    """A store that runs `meanwhile` once, after it judged the lock and before it takes the right to remove it."""

    def __init__(self, path: Path, meanwhile: Callable[[], object]) -> None:
        super().__init__(path, now=lambda: NOW)
        self._meanwhile: Callable[[], object] | None = meanwhile

    @contextlib.contextmanager
    def _removing(self, instance: bytes) -> Iterator[bool]:
        meanwhile, self._meanwhile = self._meanwhile, None
        if meanwhile is not None:
            meanwhile()
        with super()._removing(instance) as removable:
            yield removable


def test_two_runs_breaking_one_stale_lock_never_remove_the_fresh_lock_the_first_took(tmp_path: Path) -> None:
    lock_path = _stale_lock(tmp_path)
    first = StateFileStore(tmp_path / "state.json", now=lambda: NOW)
    # The second judged `old` stale; before it removes it, the first breaks it and takes the lock.
    second = _Breaker(tmp_path / "state.json", lambda: first.acquire_lock("first", break_stale=True))
    assert second.acquire_lock("second", break_stale=True) == (False, "first", False)
    assert json.loads(lock_path.read_text())["run_id"] == "first"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["state.json.lock"]


def test_a_stale_holder_releasing_while_its_lock_is_broken_never_removes_the_new_lock(tmp_path: Path) -> None:
    lock_path = _stale_lock(tmp_path)
    old = StateFileStore(tmp_path / "state.json", now=lambda: NOW)
    breaker = StateFileStore(tmp_path / "state.json", now=lambda: NOW)
    original = breaker._removing

    @contextlib.contextmanager
    def releasing_meanwhile(instance: bytes) -> Iterator[bool]:
        with original(instance) as removable:
            # The breaker holds the right to remove `old`; `old` releasing now leaves it be.
            old.release_lock("old")
            assert lock_path.exists()
            yield removable

    breaker._removing = releasing_meanwhile  # type: ignore[method-assign]
    assert breaker.acquire_lock("new", break_stale=True) == (True, "old", True)
    old.release_lock("old")
    assert json.loads(lock_path.read_text())["run_id"] == "new"


def test_a_lock_released_between_the_create_and_the_read_is_taken_like_a_free_one(tmp_path: Path) -> None:
    store = StateFileStore(tmp_path / "state.json", now=lambda: NOW)
    lock_path = tmp_path / "state.json.lock"
    lock_path.write_text(json.dumps({"run_id": "gone", "created_at": NOW.isoformat()}))
    read = store._lock_bytes

    def released_first() -> bytes | None:
        lock_path.unlink(missing_ok=True)
        store._lock_bytes = read  # type: ignore[method-assign]
        return None

    store._lock_bytes = released_first  # type: ignore[method-assign]
    assert store.acquire_lock("next", break_stale=False) == (True, None, False)
    assert json.loads(lock_path.read_text())["run_id"] == "next"


def test_a_lock_that_cannot_be_decoded_names_no_holder_and_is_never_broken(tmp_path: Path) -> None:
    store = StateFileStore(tmp_path / "state.json", now=lambda: NOW)
    (tmp_path / "state.json.lock").write_bytes(b"\xff not json")
    assert store.acquire_lock("next", break_stale=True) == (False, None, False)
    store.release_lock("next")
    assert (tmp_path / "state.json.lock").exists()
