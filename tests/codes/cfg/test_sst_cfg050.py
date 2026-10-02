"""SST-CFG050: the target asks for authentication SST does not support."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg050_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(target_project(tmp_path, "oauth_client_id: id")))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG050", Severity.ERROR)
    assert (
        diagnostic.message == "target 'x': oauth_client_id ask dbt to exchange a refresh token, which SST does not do"
    )


def test_sst_cfg050_silent(tmp_path: Path) -> None:
    target = load_profile_target(project_paths(target_project(tmp_path, "authenticator: externalbrowser")))
    assert target.authentication == "externalbrowser"
