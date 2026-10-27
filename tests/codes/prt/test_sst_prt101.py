"""SST-PRT101: a selector contains a comma; union is spelled with spaces and intersection is unsupported."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt101_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = invoke_json(["validate", *common(project_copy(tmp_path)), "--select", "a,b"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT101", "error")
    assert diagnostic["message"] == "selector 'a,b' contains a comma"


def test_sst_prt101_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    args = ["validate", *common(project), "--select", "jaffle_menu", "--select", "jaffle_sales"]
    exit_code, diagnostics = invoke_json(args)
    assert exit_code == 0 and "SST-PRT101" not in [item["code"] for item in diagnostics]
