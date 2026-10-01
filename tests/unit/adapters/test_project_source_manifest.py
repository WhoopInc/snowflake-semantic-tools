"""Which dbt manifest a project source reads, and how often it runs dbt to get it."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters import project_source
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs, YamlProjectSource

FIXTURE_MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "reference_project_manifest.json"


def _project(tmp_path: Path) -> Path:
    (tmp_path / "dbt_project.yml").write_text("name: fixture\nprofile: sst\ntarget-path: build\n", encoding="utf-8")
    (tmp_path / "build").mkdir()
    shutil.copy(FIXTURE_MANIFEST, tmp_path / "build" / "manifest.json")
    return tmp_path


def test_the_manifest_is_read_under_target_path_after_one_dbt_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = _project(tmp_path)
    parses: list[tuple[Path, str | None]] = []
    monkeypatch.setattr(project_source, "run_dbt_parse", lambda directory, target: parses.append((directory, target)))
    source = YamlProjectSource(project, target_name="dev")
    assert source.manifest_file() == project / "build" / "manifest.json"
    first = source.dbt_catalog()
    second = source.dbt_catalog()
    assert parses == [(project, "dev")]
    assert first == second and first.model("customers") is not None


def test_a_given_manifest_is_read_without_running_dbt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = _project(tmp_path)
    monkeypatch.setattr(project_source, "run_dbt_parse", lambda *_: pytest.fail("dbt must not run"))
    inputs = YamlProjectInputs(project, target_name=None, manifest_path=FIXTURE_MANIFEST, git_sha=lambda: "sha")
    assert inputs.dbt_catalog().model("orders") is not None
    never = YamlProjectSource(project, invoke_dbt=False)
    assert never.dbt_catalog().model("orders") is not None
