"""SST-CFG012: a connection is about to open, and the target sets no account or no user."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import target_project
from tests.helpers.projects import project_paths


def test_sst_cfg012_fires(tmp_path: Path) -> None:
    target = load_profile_target(project_paths(target_project(tmp_path, "account: acct")))
    with pytest.raises(ProjectError) as raised:
        _ = target.connection_params  # reading the arguments is what opening a connection does
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG012", Severity.ERROR)
    assert diagnostic.message == "profile 'test' has no user"


def test_sst_cfg012_silent(tmp_path: Path) -> None:
    # Offline, a target with no credential at all still resolves: only connecting needs one.
    offline = load_profile_target(project_paths(target_project(tmp_path)))
    assert offline.identity.database.sql == "DB"
    online = load_profile_target(project_paths(target_project(tmp_path, "account: acct\nuser: me")))
    assert online.connection_params["user"] == "me"
