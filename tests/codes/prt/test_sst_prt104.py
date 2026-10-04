"""SST-PRT104: two flags that exclude each other were both given."""

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


def test_sst_prt104_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = _refusal(["validate", *common(project_copy(tmp_path)), "--verbose", "--quiet"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT104", "error")
    assert diagnostic["message"] == "--verbose and --quiet cannot be combined"
    _, [partial] = _refusal(["plan", *common(project_copy(tmp_path / "p")), "--partial", "--prune"])
    assert partial["message"] == "--partial and --prune cannot be combined"


def test_sst_prt104_silent(tmp_path: Path) -> None:
    exit_code, _ = _refusal(["validate", *common(project_copy(tmp_path)), "--quiet"])
    assert exit_code == 0
