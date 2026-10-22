"""SST-CFG056: no file names the project's profiles.yml profile, or the two that do disagree."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import resolve_profile_name
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.coco_desktop_profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg056_fires(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        resolve_profile_name(project_paths(tmp_path))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG056", Severity.ERROR)
    assert diagnostic.message == "no profiles.yml profile is named: dbt_project.yml declares no profile"
    assert diagnostic.origin is not None and diagnostic.origin.file == "dbt_project.yml"


def test_sst_cfg056_fires_without_dbt_or_target_profile(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("project: {}\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        resolve_profile_name(project_paths(tmp_path))
    assert raised.value.diagnostics[0].message == (
        "no profiles.yml profile is named: the project has no dbt_project.yml, and project.target_profile is not set"
    )


def test_sst_cfg056_silent(tmp_path: Path) -> None:
    assert resolve_profile_name(project_paths(target_project(tmp_path))) == "test"
