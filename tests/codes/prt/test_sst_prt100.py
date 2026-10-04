"""SST-PRT100: a flag, value, or combination is not accepted; the run exits 3 and executes nothing."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def _refusal(args: list[str]) -> tuple[int, list[dict[str, object]]]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    envelope = json.loads(result.output)
    return result.exit_code, envelope["diagnostics"]


def test_sst_prt100_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = _refusal(["validate", *common(project_copy(tmp_path)), "-s", "ANALYTICS"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT100", "error")
    assert diagnostic["message"] == (
        "-s is reserved: it meant --schema in SST 0.3 and becomes --select in 1.0.1; write --schema or --select"
    )
    unknown_code, [unknown] = _refusal(["validate", "--no-such-flag"])
    assert (unknown_code, unknown["code"]) == (3, "SST-PRT100")


def test_sst_prt100_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    exit_code, diagnostics = _refusal(["validate", *common(project), "-t", "dev", "-o", "json"])
    assert exit_code == 0 and "SST-PRT100" not in [item["code"] for item in diagnostics]
