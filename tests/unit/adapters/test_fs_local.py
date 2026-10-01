"""The local stores name the failure when a manifest or state file exists and cannot be used."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore, PlanFileStore, StateFileStore
from snowflake_semantic_tools.domain.state import MANIFEST_SCHEMA_VERSION, PLAN_SCHEMA_VERSION, STATE_SCHEMA_VERSION


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


def _manifest_with_artifact(**changes: object) -> dict[str, object]:
    entry: dict[str, object] = {
        "type": "semantic_view",
        "name": "v",
        "fingerprint": "f" * 64,
        "render": {"dialect": "ddl", "byte_length": 1},
        "publish_target": {"object_type": "SEMANTIC VIEW", "qualified_name": "DB.S.V"},
    }
    entry.update(changes)
    for key in [key for key, value in changes.items() if value is None]:
        del entry[key]
    return {"schema_version": MANIFEST_SCHEMA_VERSION, "artifacts": {"semantic_view:v": entry}}


@pytest.mark.parametrize(
    ("document", "detail"),
    [
        (_manifest_with_artifact(type=None), "missing key 'type'"),
        (_manifest_with_artifact(source_files=1), "'int' object is not iterable"),
        (_manifest_with_artifact(render={"byte_length": [1]}), "int() argument must be"),
        (_manifest_with_artifact(render={"byte_length": float("inf")}), "cannot convert float infinity"),
        (_manifest_with_artifact(publish_target={"object_type": "SEMANTIC VIEW"}), "missing key 'qualified_name'"),
    ],
)
def test_a_manifest_of_the_wrong_shape_is_unreadable_not_a_crash(tmp_path: Path, document: object, detail: str) -> None:
    assert _code(ManifestFileStore(_write(tmp_path / "manifest.json", document))) == "SST-MAN002"
    with pytest.raises(ProjectError, match=re.escape(detail)):
        ManifestFileStore(tmp_path / "manifest.json").read()


@pytest.mark.parametrize(
    ("document", "code"),
    [
        ("{not json", "SST-MAN022"),
        ([], "SST-MAN022"),
        ({"schema_version": "two"}, "SST-MAN023"),
        ({"schema_version": STATE_SCHEMA_VERSION + 1}, "SST-MAN023"),
        ({"schema_version": 0}, "SST-MAN023"),
        ({"schema_version": STATE_SCHEMA_VERSION, "applied": []}, "SST-MAN022"),
        ({}, "SST-MAN023"),
        ('"state"', "SST-MAN022"),
        ({"schema_version": STATE_SCHEMA_VERSION}, "SST-MAN022"),
        ({"schema_version": STATE_SCHEMA_VERSION, "applied": {"k": "entry"}}, "SST-MAN022"),
        ({"schema_version": STATE_SCHEMA_VERSION, "applied": {}, "target": "dev"}, "SST-MAN022"),
        ({"schema_version": STATE_SCHEMA_VERSION, "applied": {"k": {"physical_resources": [[]]}}}, "SST-MAN022"),
    ],
)
def test_an_unusable_state_file_names_why(tmp_path: Path, document: object, code: str) -> None:
    assert _code(StateFileStore(_write(tmp_path / "state.dev.json", document))) == code


def test_a_saved_plan_keeps_its_own_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="saved plan must be an object"):
        PlanFileStore(_write(tmp_path / "plan.json", [])).read()


@pytest.mark.parametrize(
    ("change", "detail"),
    [
        ({}, "missing key 'key'"),
        ({"key": "k", "artifact_type": "t", "action": "create", "reason": "new", "order": None}, "int() argument"),
        ({"key": "k", "artifact_type": "t", "action": "a", "reason": "r", "order": float("inf")}, "infinity"),
        (
            {"key": "k", "artifact_type": "t", "action": "a", "reason": "r", "order": 0, "statement_hashes": 1},
            "'int' object is not iterable",
        ),
    ],
)
def test_a_saved_plan_of_the_wrong_shape_is_its_own_error_not_a_crash(
    tmp_path: Path, change: dict[str, object], detail: str
) -> None:
    document = {"schema_version": PLAN_SCHEMA_VERSION, "changes": [change]}
    with pytest.raises(ValueError, match=re.escape(detail)) as raised:
        PlanFileStore(_write(tmp_path / "plan.json", document)).read()
    # No registered code names an unusable saved plan, so the ValueError names the file.
    assert str(raised.value).startswith(f"{tmp_path / 'plan.json'} has the wrong shape: ")
