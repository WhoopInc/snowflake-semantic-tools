"""`tests.helpers.manifest_capture`: a captured manifest keeps nothing of the run that wrote it."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers import manifest_capture
from tests.helpers.manifest_capture import FIXTURES, PROJECT_ROOT, fixture_path, scrub


def _captured(project: Path) -> dict[str, object]:
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.12.0",
            "generated_at": "2000-01-02T03:04:05Z",
            "invocation_id": "d27147fa-b12c-4062-af3b-f0c2fa9758dd",
            "env": {"DBT_ENV_CUSTOM_ENV_RUNNER": "laptop"},
            "user_id": "f00d",
        },
        "nodes": {
            "seed.p.raw": {"root_path": str(project), "created_at": 1.5, "path": f"{project.resolve()}/seeds/raw.csv"},
        },
        "macros": {"macro.dbt.ref": {"created_at": 2}},
    }


def test_scrub_replaces_the_capture_root_and_every_value_of_the_run(tmp_path: Path) -> None:
    scrubbed = scrub(_captured(tmp_path), tmp_path)
    assert scrubbed["nodes"] == {
        "seed.p.raw": {"root_path": PROJECT_ROOT, "created_at": 0.0, "path": f"{PROJECT_ROOT}/seeds/raw.csv"}
    }
    assert scrubbed["metadata"] == {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "dbt_version": "1.12.0",
        "generated_at": "1970-01-01T00:00:00Z",
        "invocation_id": "00000000-0000-0000-0000-000000000000",
        "env": {},
        "user_id": None,
    }
    assert scrubbed["macros"] == {}
    assert fixture_path(scrubbed) == FIXTURES / "v12" / "dbt-1.12.0" / "manifest.json"


def test_a_manifest_without_a_schema_version_has_no_fixture_path() -> None:
    with pytest.raises(ValueError, match="names no schema version"):
        fixture_path({"metadata": {"dbt_schema_version": "v12"}})


def test_main_writes_the_scrubbed_fixture_where_it_belongs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(manifest_capture, "FIXTURES", tmp_path / "fixtures")
    written = tmp_path / "manifest.json"
    written.write_text(json.dumps(_captured(tmp_path / "project")), encoding="utf-8")
    assert manifest_capture.main([str(written), str(tmp_path / "project")]) == 0
    path = tmp_path / "fixtures" / "v12" / "dbt-1.12.0" / "manifest.json"
    assert capsys.readouterr().out == f"{path}\n"
    assert json.loads(path.read_text(encoding="utf-8"))["nodes"]["seed.p.raw"]["root_path"] == PROJECT_ROOT
    assert manifest_capture.main([str(written)]) == 2


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("v*/dbt-*/manifest.json")), ids=lambda path: path.parent.name)
def test_every_committed_fixture_is_already_scrubbed(path: Path) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    assert scrub(document, Path("/nowhere")) == document
    assert path.read_text(encoding="utf-8") == json.dumps(document, indent=1, sort_keys=True) + "\n"
