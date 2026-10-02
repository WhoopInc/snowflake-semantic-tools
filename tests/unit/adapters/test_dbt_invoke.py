"""How SST runs dbt: an argument list with no shell, and the project settings the run reads."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, installation_type, subprocess_runner
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest


def test_the_runner_runs_an_argument_list_in_the_directory_and_captures_its_output(tmp_path: Path) -> None:
    completed = subprocess_runner([sys.executable, "-c", "import os; print(os.getcwd())"], tmp_path)
    assert completed == CompletedRun(0, f"{tmp_path.resolve()}\n", "")
    with pytest.raises(OSError):
        subprocess_runner([str(tmp_path / "no-such-dbt"), "--version"], tmp_path)


def test_installations_are_told_apart_by_what_dbt_prints() -> None:
    assert installation_type("Core:\n  - installed: 1.11.2\n") == "core"
    assert installation_type("installed version: dbt-core 1.8") == "core"
    assert installation_type("dbt Cloud CLI - 0.38.6") == "cloud"
    assert installation_type("something else") is None


def test_a_project_source_parses_once_and_reports_dbts_warnings_with_the_project(tmp_path: Path) -> None:
    project = SmallProject(tmp_path)
    project.write()
    dbt = FakeDbt(
        version=CompletedRun(0, "dbt-fusion 2.0\n", ""), writes=(tmp_path / "target" / "manifest.json", manifest())
    )
    loaded = YamlProjectSource(tmp_path, dbt_runner=dbt).load_project()
    assert [item.code for item in loaded.diagnostics][:1] == ["SST-DBT020"]
    assert [run[1] for run in dbt.runs] == ["--version", "parse"]


def test_defer_auto_compile_in_the_config_is_read_before_dbt_parses(tmp_path: Path) -> None:
    config = "project:\n  semantic_models_dir: semantic_models\ndefer:\n  auto_compile: true\n"
    SmallProject(tmp_path, files={"sst_config.yml": config}).write()
    dbt = FakeDbt(version=CompletedRun(0, "dbt Cloud CLI - 0.38.6\n", ""))
    with pytest.raises(ProjectError) as raised:
        YamlProjectSource(tmp_path, dbt_runner=dbt).dbt_catalog()
    assert [item.code for item in raised.value.diagnostics] == ["SST-DBT021"]
