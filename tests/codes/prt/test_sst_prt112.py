"""SST-PRT112: `sst lineage NODE` names nothing the lineage graph holds."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import compile_project
from tests.helpers.reference_project import project_copy


def test_sst_prt112_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    exit_code, [diagnostic] = invoke_json(["lineage", "--project-dir", str(project), "model:order"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT112", "error")
    assert diagnostic["message"] == "no lineage node is named 'model:order'"
    assert diagnostic["suggestion"].endswith("; did you mean model:orders?")
    _, [far] = invoke_json(["lineage", "--project-dir", str(project), "zzzz"])
    assert not far["suggestion"].endswith("?")


def test_sst_prt112_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    exit_code, diagnostics = invoke_json(["lineage", "--project-dir", str(project), "model:orders"])
    assert (exit_code, [item["code"] for item in diagnostics]) == (0, [])
