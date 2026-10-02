"""SST-PRT011: a credential written for the target renders to nothing."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths

PASSWORD = "password: \"{{ env_var('SST_TEST_PASSWORD', '') }}\""


def test_sst_prt011_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(target_project(tmp_path, PASSWORD)))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRT011", Severity.ERROR)
    assert diagnostic.message == "x.password resolved to an empty credential"


def test_sst_prt011_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SST_TEST_PASSWORD", "hunter2")
    target = load_profile_target(project_paths(target_project(tmp_path, PASSWORD)))
    assert target.authentication == "password" and target.secrets == ("hunter2",)
