"""SST-CFG011: the resolved dbt target is for another adapter than Snowflake."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg011_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(target_project(tmp_path, adapter="postgres")))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG011", Severity.ERROR)
    assert diagnostic.message == "profile 'test' declares type 'postgres'"
    assert diagnostic.subject == "config:profiles.yml"


def test_sst_cfg011_silent(tmp_path: Path) -> None:
    assert load_profile_target(project_paths(target_project(tmp_path))).profile_name == "test"
