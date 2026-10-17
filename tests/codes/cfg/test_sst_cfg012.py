"""SST-CFG012: the target sets no database or schema, or no account or user as a connection opens."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.dbt.project import resolve_target
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


def _without(root: Path, *keys: str) -> Path:
    project = target_project(root)
    profiles = project / "profiles.yml"
    text = profiles.read_text(encoding="utf-8")
    for key in keys:
        text = text.replace({"database": "      database: DB\n", "schema": "      schema: S\n"}[key], "")
    profiles.write_text(text, encoding="utf-8")
    return project


def test_sst_cfg012_fires_for_a_target_without_a_schema(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(_without(tmp_path, "schema")))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG012", Severity.ERROR)
    assert diagnostic.message == "profile 'test' has no schema on target 'x'"
    assert diagnostic.subject == "config:profiles.yml"


def test_sst_cfg012_fires_for_both_where_the_dbt_target_is_resolved(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        resolve_target(project_paths(_without(tmp_path, "database", "schema")))
    assert [item.message for item in raised.value.diagnostics] == [
        "profile 'test' has no database and schema on target 'x'"
    ]


def test_sst_cfg012_silent_for_a_target_with_its_location(tmp_path: Path) -> None:
    assert resolve_target(project_paths(target_project(tmp_path))).schema == "S"
