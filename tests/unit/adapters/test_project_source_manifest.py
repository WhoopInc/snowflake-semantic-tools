"""Which dbt manifest a project source reads, and how often it runs dbt to get it."""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs, YamlProjectSource

FIXTURE_MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "reference_project_manifest.json"
CORE = CompletedRun(0, "Core:\n  - installed: 1.11.2\n", "")


def _project(tmp_path: Path) -> Path:
    (tmp_path / "dbt_project.yml").write_text("name: fixture\nprofile: sst\ntarget-path: build\n", encoding="utf-8")
    (tmp_path / "build").mkdir()
    shutil.copy(FIXTURE_MANIFEST, tmp_path / "build" / "manifest.json")
    return tmp_path


def test_the_manifest_is_read_under_target_path_after_one_dbt_parse(tmp_path: Path) -> None:
    project = _project(tmp_path)
    runs: list[tuple[str, ...]] = []

    def runner(argv: Sequence[str], cwd: Path) -> CompletedRun:
        runs.append(tuple(argv))
        return CORE if argv[1] == "--version" else CompletedRun(0, "", "")

    source = YamlProjectSource(project, target_name="dev", dbt_runner=runner)
    assert source.manifest_file() == project / "build" / "manifest.json"
    first = source.dbt_catalog()
    second = source.dbt_catalog()
    assert runs == [
        ("dbt", "--version"),
        ("dbt", "parse", "--project-dir", str(project), "--profiles-dir", str(project), "--target", "dev"),
    ]
    assert first == second and first.model("customers") is not None


def test_a_given_manifest_is_read_without_running_dbt(tmp_path: Path) -> None:
    project = _project(tmp_path)

    def runner(argv: Sequence[str], cwd: Path) -> CompletedRun:
        pytest.fail("dbt must not run")

    inputs = YamlProjectInputs(
        project, target_name=None, manifest_path=FIXTURE_MANIFEST, git_sha=lambda: "sha", dbt_runner=runner
    )
    assert inputs.dbt_catalog().model("orders") is not None
    never = YamlProjectSource(project, invoke_dbt=False, dbt_runner=runner)
    assert never.dbt_catalog().model("orders") is not None
