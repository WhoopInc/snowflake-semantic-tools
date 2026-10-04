"""SST-MAN007: `sst compile` could not write the manifest."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.reference_project import compile_json, project_copy


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
