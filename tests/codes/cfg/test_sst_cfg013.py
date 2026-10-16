"""SST-CFG013: a field SST reads names an unset environment variable with no default."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import declared_targets, load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.profile_projects import default_target_project, target_project
from tests.helpers.projects import project_paths

ACCOUNT = "account: \"{{ env_var('SST_TEST_ACCOUNT') }}\""


def test_sst_cfg013_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(target_project(tmp_path, ACCOUNT)))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG013", Severity.ERROR)
    assert diagnostic.message == "env_var('SST_TEST_ACCOUNT') is unset and has no default"


def test_sst_cfg013_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SST_TEST_ACCOUNT", "acct")
    assert load_profile_target(project_paths(target_project(tmp_path, ACCOUNT))).identity.account_locator == "acct"
    # A field SST does not read is left as written, so its variable may be unset.
    unread = "threads: \"{{ env_var('SST_TEST_UNSET') | as_number }}\""
    assert load_profile_target(project_paths(target_project(tmp_path, unread))).profile_name == "test"


def test_sst_cfg013_fires_for_the_default_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SST_TEST_TARGET", raising=False)
    project = project_paths(default_target_project(tmp_path, "\"{{ env_var('SST_TEST_TARGET') }}\""))
    for read in (load_profile_target, declared_targets):
        with pytest.raises(ProjectError) as raised:
            read(project)
        assert [item.code for item in raised.value.diagnostics] == ["SST-CFG013"]
