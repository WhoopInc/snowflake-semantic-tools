"""The local stores name the failure when a manifest or state file exists and cannot be used."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore, PlanFileStore, StateFileStore
from snowflake_semantic_tools.adapters.project import ProjectError
from snowflake_semantic_tools.domain.state.model import MANIFEST_SCHEMA_VERSION, STATE_SCHEMA_VERSION


def _write(path: Path, value: object) -> Path:
    path.write_text(value if isinstance(value, str) else json.dumps(value), encoding="utf-8")
    return path


def _code(store: ManifestFileStore | StateFileStore) -> str:
    with pytest.raises(ProjectError) as raised:
        store.read()
    [diagnostic] = raised.value.diagnostics
    assert str(store.path) in diagnostic.message or diagnostic.code == "SST-MAN005"
    return diagnostic.code


def test_an_absent_file_is_none_not_an_error(tmp_path: Path) -> None:
    assert ManifestFileStore(tmp_path / "manifest.json").read() is None
    assert StateFileStore(tmp_path / "state.dev.json").read() is None


@pytest.mark.parametrize(
    ("document", "code"),
    [
        ("{not json", "SST-MAN002"),
        ([], "SST-MAN002"),
        ({"artifacts": {}}, "SST-MAN003"),
        ({"schema_version": MANIFEST_SCHEMA_VERSION}, "SST-MAN003"),
        ({"schema_version": MANIFEST_SCHEMA_VERSION + 1, "artifacts": {}}, "SST-MAN203"),
        ({"schema_version": 0, "artifacts": {}}, "SST-MAN202"),
        ({"schema_version": MANIFEST_SCHEMA_VERSION, "manifest_id": "stale", "artifacts": {}}, "SST-MAN005"),
        ({"schema_version": MANIFEST_SCHEMA_VERSION, "artifacts": {"a": []}}, "SST-MAN002"),
    ],
)
def test_an_unusable_manifest_names_why(tmp_path: Path, document: object, code: str) -> None:
    assert _code(ManifestFileStore(_write(tmp_path / "manifest.json", document))) == code


@pytest.mark.parametrize(
    ("document", "code"),
    [
        ("{not json", "SST-MAN022"),
        ([], "SST-MAN022"),
        ({"schema_version": "two"}, "SST-MAN023"),
        ({"schema_version": STATE_SCHEMA_VERSION + 1}, "SST-MAN023"),
        ({"schema_version": 0}, "SST-MAN023"),
        ({"schema_version": STATE_SCHEMA_VERSION, "applied": []}, "SST-MAN022"),
    ],
)
def test_an_unusable_state_file_names_why(tmp_path: Path, document: object, code: str) -> None:
    assert _code(StateFileStore(_write(tmp_path / "state.dev.json", document))) == code


def test_a_saved_plan_keeps_its_own_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="saved plan must be an object"):
        PlanFileStore(_write(tmp_path / "plan.json", [])).read()
