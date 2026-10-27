"""SST-PRT110: `--select` or `--exclude` is passed to a command that takes no selector."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt110_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = invoke_json(["debug", *common(project_copy(tmp_path)), "--select", "orders"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT110", "error")
    assert diagnostic["message"] == "sst debug takes no selector; 'orders' is not accepted"


def test_sst_prt110_silent(tmp_path: Path) -> None:
    exit_code, _ = invoke_json(["debug", *common(project_copy(tmp_path)), "--no-connect"])
    assert exit_code == 0
