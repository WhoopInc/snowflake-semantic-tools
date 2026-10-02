"""SST-CFG051: a `profiles.yml` target turns off OCSP certificate revocation checks."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg051_fires(tmp_path: Path) -> None:
    target = load_profile_target(
        project_paths(target_project(tmp_path, "account: a\nuser: u\npassword: x\ninsecure_mode: true"))
    )
    [diagnostic] = target.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG051", Severity.WARNING)
    assert diagnostic.message == (
        "target 'x': insecure_mode is true, so OCSP certificate revocation checks are off for this connection"
    )
    # Reported, not refused: the connection still honours the setting.
    assert target.connection_params["insecure_mode"] is True


def test_sst_cfg051_silent(tmp_path: Path) -> None:
    assert (
        load_profile_target(project_paths(target_project(tmp_path, "password: x\ninsecure_mode: false"))).diagnostics
        == ()
    )
