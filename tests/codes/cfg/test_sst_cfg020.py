"""SST-CFG020: a per-group `tools.<group>` override names no declared group, or one with no `define:` members."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def _compile(project: Path, tools: str) -> dict[str, Any]:
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8").replace("\ntools:\n", "\ntools:\n" + tools, 1), encoding="utf-8"
    )
    result = CliRunner().invoke(cli, ["compile", *common(project), "--output", "json"])
    envelope: dict[str, Any] = json.loads(result.output)
    return envelope


def test_sst_cfg020_fires(tmp_path: Path) -> None:
    envelope = _compile(project_copy(tmp_path), "  jaffle_partner:\n    +schema: ELSEWHERE\n  nope: {}\n")
    found = [item for item in envelope["diagnostics"] if item["code"] == "SST-CFG020"]
    assert [(item["severity"], item["message"]) for item in found] == [
        (
            "error",
            "tools: override names group 'jaffle_partner', which has no define: members, so the override "
            "would change nothing",
        ),
        ("error", "tools: override names group 'nope', which is not declared"),
    ]
    assert envelope["exit_code"] == 1


def test_sst_cfg020_silent(tmp_path: Path) -> None:
    envelope = _compile(project_copy(tmp_path), "  jaffle_platform:\n    +schema: TOOLS_ELSEWHERE\n")
    assert envelope["exit_code"] == 0, envelope["diagnostics"]
    targets = {item["artifact_key"]: item["target"] for item in envelope["data"]["artifacts"]}
    assert targets["tool:menu_docs_search"] == "SST_REF_DEV.TOOLS_ELSEWHERE.MENU_DOCS_SEARCH"
