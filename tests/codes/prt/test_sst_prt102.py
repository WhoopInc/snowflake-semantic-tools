"""SST-PRT102: a selector's prefix is not a registered kind, or it globs where only a name may."""

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


def test_sst_prt102_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for selector in ("view:orders", "type:*", "tool:*"):
        exit_code, [diagnostic] = _refusal(["validate", *common(project), "--select", selector])
        assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT102", "error")
        assert diagnostic["message"] == f"selector '{selector}' names an unknown kind"


def test_sst_prt102_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for selector in ("type:agent", "jaffle_*", "agent:jaffle_minimal_agent", "path:semantic_models/*"):
        exit_code, diagnostics = _refusal(["validate", *common(project), "--select", selector])
        assert exit_code == 0, (selector, diagnostics)
