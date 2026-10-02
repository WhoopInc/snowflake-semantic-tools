"""SST-CFG046: configuration that needs dbt is present in a project without `dbt_project.yml`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.projects import project_paths

BASE = "validation:\n  snowflake_syntax_check: false\n"


def test_sst_cfg046_fires(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text(BASE + "semantic_views: {}\n", encoding="utf-8")
    [diagnostic] = load_project_config(project_paths(tmp_path)).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG046", Severity.ERROR)
    assert diagnostic.message == "semantic_views requires a dbt project, and the project has no dbt_project.yml"
    assert diagnostic.subject == "config:semantic_views"


def test_sst_cfg046_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text(BASE + "semantic_views: {}\n", encoding="utf-8")
    (tmp_path / "dbt_project.yml").write_text("profile: p\n", encoding="utf-8")
    assert load_project_config(project_paths(tmp_path)).diagnostics == ()
