"""SST-CFG049: a `profiles.yml` value holds a template other than `env_var()`, or the wrong type."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg049_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(target_project(tmp_path, "port: four")))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG049", Severity.ERROR)
    assert diagnostic.message == "target 'x': 'port' must be a whole number"


def test_sst_cfg049_silent(tmp_path: Path) -> None:
    assert load_profile_target(project_paths(target_project(tmp_path, 'port: "443"'))).profile_name == "test"
