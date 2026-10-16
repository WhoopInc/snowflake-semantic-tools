"""SST-CFG047: a `project.*_dir` key names a path that is not a directory in the project."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import validate_json
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import config_edited

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


def test_sst_cfg047_for_the_semantic_models_dir_is_reported_before_discovery(tmp_path: Path) -> None:
    edit = ("  semantic_models_dir: semantic_models", "  semantic_models_dir: nope_dir")
    code, diagnostics = validate_json(config_edited(tmp_path, edit))
    codes = [item["code"] for item in diagnostics]
    # Discovery does not look for a directory the configuration already refused.
    assert code == 1 and "SST-CFG047" in codes and "SST-DIS001" not in codes
