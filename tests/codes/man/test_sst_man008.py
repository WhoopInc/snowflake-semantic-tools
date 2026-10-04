"""SST-MAN008: `sst compile` failed before it could write a manifest."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.reference_project import DBT_MANIFEST, break_menu_view, project_copy


def compile_json(project: Path) -> tuple[int, list[dict[str, object]]]:
    result = CliRunner().invoke(
        cli, ["compile", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "--output", "json"]
    )
    return result.exit_code, json.loads(result.output)["diagnostics"]


def test_sst_man008_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    exit_code, diagnostics = compile_json(project)
    [diagnostic] = [item for item in diagnostics if item["code"] == "SST-MAN008"]
    errors = sum(item["severity"] == "error" for item in diagnostics) - 1
    assert exit_code == 1 and diagnostic["severity"] == "error"
    assert diagnostic["message"] == f"compile failed: {errors} error(s); no manifest was written"
    assert not (project / "target" / "sst" / "manifest.json").exists()


def test_sst_man008_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    exit_code, diagnostics = compile_json(project)
    assert exit_code == 0 and "SST-MAN008" not in [item["code"] for item in diagnostics]
    assert (project / "target" / "sst" / "manifest.json").is_file()
