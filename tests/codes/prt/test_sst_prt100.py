"""SST-PRT100: a flag, value, or combination is not accepted; the run exits 3 and executes nothing."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt100_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = invoke_json(["validate", *common(project_copy(tmp_path)), "-s", "ANALYTICS"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT100", "error")
    assert diagnostic["message"] == (
        "-s is reserved: it meant --schema in SST 0.3 and becomes --select in 1.0.1; write --schema or --select"
    )
    unknown_code, [unknown] = invoke_json(["validate", "--no-such-flag"])
    assert (unknown_code, unknown["code"]) == (3, "SST-PRT100")


def test_sst_prt100_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    exit_code, diagnostics = invoke_json(["validate", *common(project), "-t", "dev", "-o", "json"])
    assert exit_code == 0 and "SST-PRT100" not in [item["code"] for item in diagnostics]
