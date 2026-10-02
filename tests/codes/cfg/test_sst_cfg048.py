"""SST-CFG048: a `profiles.yml` target carries a field SST does not read."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg048_fires(tmp_path: Path) -> None:
    [diagnostic] = load_profile_target(project_paths(target_project(tmp_path, "pasword: x"))).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG048", Severity.WARNING)
    assert diagnostic.message == "target 'x': 'pasword' is not a setting SST reads, so it is ignored"


def test_sst_cfg048_silent(tmp_path: Path) -> None:
    assert load_profile_target(project_paths(target_project(tmp_path, "password: x\nthreads: 4"))).diagnostics == ()
