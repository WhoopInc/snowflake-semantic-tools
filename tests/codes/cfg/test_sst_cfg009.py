"""SST-CFG009: no `profiles.yml` exists anywhere the run searches."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_cfg009_fires(tmp_path: Path) -> None:
    paths = locate_project(tmp_path, required=False)
    with pytest.raises(ProjectError) as raised:
        paths.profiles_file()
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG009", Severity.ERROR)
    assert diagnostic.message == "no profiles.yml at any searched location"
    assert diagnostic.subject == "config:profiles.yml"
    assert str(tmp_path / "profiles.yml") in str(raised.value)


def test_sst_cfg009_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    profiles = tmp_path / "dbt"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text("p: {}\n", encoding="utf-8")
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles))
    paths = locate_project(tmp_path, required=False)
    assert paths.profiles_file() == profiles / "profiles.yml"
    explicit = locate_project(tmp_path, required=False, profiles_dir=tmp_path)
    (tmp_path / "profiles.yml").write_text("p: {}\n", encoding="utf-8")
    assert explicit.profiles_file() == tmp_path / "profiles.yml"
