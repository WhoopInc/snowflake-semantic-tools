"""SST-DIS009: a `model-paths` entry dbt_project.yml sets is not a directory."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.project import check_model_paths
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.adapters.yaml.parse import read_yaml_mapping
from snowflake_semantic_tools.domain.diagnostics import Severity


def dbt_project(tmp_path: Path, paths: str) -> Path:
    (tmp_path / "dbt_project.yml").write_text(f"name: p\nmodel-paths: {paths}\n", encoding="utf-8")
    return tmp_path


def test_sst_dis009_fires(tmp_path: Path) -> None:
    # Refused before dbt is run, so no dbt is needed to reach it.
    with pytest.raises(ProjectError) as caught:
        YamlProjectSource(dbt_project(tmp_path, "[models, staging]")).dbt_catalog()
    diagnostics = caught.value.diagnostics
    assert [(item.code, item.severity) for item in diagnostics] == [("SST-DIS009", Severity.ERROR)] * 2
    assert diagnostics[1].message == "dbt model-paths entry staging does not exist"


def test_sst_dis009_silent(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    check_model_paths(dbt_project(tmp_path, "[models]"), read_yaml_mapping)
    (tmp_path / "dbt_project.yml").write_text("name: p\n", encoding="utf-8")
    check_model_paths(tmp_path, read_yaml_mapping)
