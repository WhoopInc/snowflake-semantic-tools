"""SST-CFG014: a connection would leave the role or the warehouse to the account's defaults."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg014_fires(tmp_path: Path) -> None:
    target = load_profile_target(project_paths(target_project(tmp_path, "warehouse: WH")))
    [diagnostic] = target.connection_warnings
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG014", Severity.WARNING)
    assert diagnostic.message == "profile 'test' leaves role empty"
    assert diagnostic.subject == "config:profiles.yml"


def test_sst_cfg014_silent(tmp_path: Path) -> None:
    target = load_profile_target(project_paths(target_project(tmp_path, "role: R\nwarehouse: WH")))
    assert target.connection_warnings == ()
