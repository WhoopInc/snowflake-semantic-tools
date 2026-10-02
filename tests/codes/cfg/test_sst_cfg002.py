"""SST-CFG002: the configuration file, or `profiles.yml`, is not valid YAML."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.projects import project_paths


def test_sst_cfg002_fires(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("project:\n  semantic_models_dir: [unclosed\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        load_project_config(project_paths(tmp_path))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG002", Severity.ERROR)
    assert diagnostic.message.startswith("sst_config.yml is not valid YAML: ")
    assert diagnostic.origin is not None and diagnostic.origin.file == "sst_config.yml"
    assert diagnostic.subject == "config:sst_config.yml"


def test_sst_cfg002_fires_for_profiles(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: p\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text("p: [unclosed\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        load_profile_target(project_paths(tmp_path))
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.code == "SST-CFG002" and diagnostic.origin == Origin("profiles.yml")


def test_sst_cfg002_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("validation:\n  snowflake_syntax_check: true\n", encoding="utf-8")
    assert [item.code for item in load_project_config(project_paths(tmp_path)).diagnostics] == []
