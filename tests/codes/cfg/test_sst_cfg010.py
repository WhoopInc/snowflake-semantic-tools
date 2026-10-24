"""SST-CFG010: the target, or the profile itself, is not declared in `profiles.yml`."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import declared_targets, load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.coco_desktop_profile_projects import default_target_project, target_project
from tests.helpers.projects import project_paths


def test_sst_cfg010_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(target_project(tmp_path)), "prod")
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG010", Severity.ERROR)
    assert diagnostic.message == "target 'prod' is absent from dbt profile 'test'"


def test_sst_cfg010_fires_for_an_undeclared_profile(tmp_path: Path) -> None:
    target_project(tmp_path)
    (tmp_path / "dbt_project.yml").write_text("profile: other\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(tmp_path))
    assert raised.value.diagnostics[0].message == "target '(default)' is absent from dbt profile 'other'"


def test_sst_cfg010_silent(tmp_path: Path) -> None:
    assert load_profile_target(project_paths(target_project(tmp_path)), "x").target_name == "x"


def test_sst_cfg010_silent_for_a_default_target_env_var_renders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # dbt renders `target:`, so SST selects the target it names, as written or by its default.
    project = project_paths(default_target_project(tmp_path, "\"{{ env_var('SST_TEST_TARGET', 'y') }}\""))
    monkeypatch.delenv("SST_TEST_TARGET", raising=False)
    assert load_profile_target(project).target_name == "y"
    assert declared_targets(project)[2] == "y"
    monkeypatch.setenv("SST_TEST_TARGET", "x")
    assert load_profile_target(project).target_name == "x"
    assert declared_targets(project)[2] == "x"
