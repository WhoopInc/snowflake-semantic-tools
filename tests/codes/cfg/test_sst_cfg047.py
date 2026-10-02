"""SST-CFG047: a `project.*_dir` key names a path that is not a directory in the project."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.projects import project_paths

CONFIG = "validation:\n  snowflake_syntax_check: false\nproject:\n  skills_dir: skillz\n"


def test_sst_cfg047_fires(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text(CONFIG, encoding="utf-8")
    [diagnostic] = load_project_config(project_paths(tmp_path)).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG047", Severity.ERROR)
    assert diagnostic.message == "project.skills_dir is skillz, which is not a directory in the project"
    assert diagnostic.origin is not None and (diagnostic.origin.file, diagnostic.origin.line) == ("sst_config.yml", 4)


def test_sst_cfg047_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text(CONFIG, encoding="utf-8")
    (tmp_path / "skillz").mkdir()
    assert load_project_config(project_paths(tmp_path)).diagnostics == ()
