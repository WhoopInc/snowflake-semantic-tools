"""SST-MAN007: `sst compile` could not write the manifest."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import DBT_MANIFEST, project_copy


def compile_json(project: Path) -> tuple[int, list[dict[str, object]]]:
    result = CliRunner().invoke(
        cli, ["compile", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "--output", "json"]
    )
    return result.exit_code, json.loads(result.output)["diagnostics"]


def test_sst_man007_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    (project / "target").mkdir(exist_ok=True)
    blocker = project / "target" / "sst"
    blocker.write_text("a file where the build directory belongs", encoding="utf-8")
    exit_code, diagnostics = compile_json(project)
    [diagnostic] = [item for item in diagnostics if item["code"] == "SST-MAN007"]
    assert exit_code == 1 and diagnostic["severity"] == "error"
    assert diagnostic["message"] == f"could not write {blocker / 'manifest.json'}: target/sst is not a folder"


def test_sst_man007_silent(tmp_path: Path) -> None:
    exit_code, diagnostics = compile_json(project_copy(tmp_path))
    assert exit_code == 0 and "SST-MAN007" not in [item["code"] for item in diagnostics]
