"""SST-DBT019: `profiles.yml` does not parse, or is not a mapping of profiles."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import profile_output
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.projects import project_paths


def _project(tmp_path: Path, profiles: str) -> Path:
    (tmp_path / "dbt_project.yml").write_text("name: x\nprofile: x\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(profiles, encoding="utf-8")
    return tmp_path


def test_sst_dbt019_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        profile_output(project_paths(_project(tmp_path, "x: [\n")))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT019", Severity.ERROR)
    assert diagnostic.message == "profiles.yml: line 2: expected the node content, but found '<stream end>'"
    assert (diagnostic.subject, diagnostic.origin) == ("config:profiles.yml", Origin("profiles.yml"))
    with pytest.raises(ProjectError) as raised:
        profile_output(project_paths(_project(tmp_path, "- x\n")))
    assert raised.value.diagnostics[0].message == "profiles.yml: the root is list, not a mapping of profiles"


def test_sst_dbt019_silent(tmp_path: Path) -> None:
    profiles = "x:\n  target: dev\n  outputs:\n    dev:\n      type: snowflake\n      database: DB\n      schema: S\n"
    assert profile_output(project_paths(_project(tmp_path, profiles)))[1] == "dev"
