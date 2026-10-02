"""SST-CFG034: `--strict` or `--no-strict` contradicts `validation.strict`; the flag wins."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def test_sst_cfg034_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["validate", *common(project), "--strict", "--output", "json"])
    [diagnostic] = [item for item in json.loads(result.output)["diagnostics"] if item["code"] == "SST-CFG034"]
    assert diagnostic["severity"] == "warning"
    assert diagnostic["message"] == "--strict true disagrees with diagnostics.strict false; the flag wins"
    assert diagnostic["location"]["file"] == "sst_config.yml"


def test_sst_cfg034_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for flags in ([], ["--no-strict"]):
        result = CliRunner().invoke(cli, ["validate", *common(project), *flags, "--output", "json"])
        assert "SST-CFG034" not in [item["code"] for item in json.loads(result.output)["diagnostics"]]
