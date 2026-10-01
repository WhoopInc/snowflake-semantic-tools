"""`sst init`, beside the JSON of the other small commands: `debug`, `list`, and `clean`."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def test_init_debug_clean_and_list_json(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--output", "json"])
    assert initialized.exit_code == 0
    assert json.loads(initialized.output)["data"]["created"] == ["sst_config.yml"]
    assert (root / "semantic_models" / "semantic_views").is_dir()

    project = project_copy(tmp_path)
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--output", "json"])
    assert debugged.exit_code == 0
    debug_data = json.loads(debugged.output)["data"]
    assert debug_data["target"] == "dev"
    # The method is shown, never a credential.
    assert isinstance(debug_data["authentication"], str) and "password" not in set(debug_data) - {"authentication"}

    compiled = CliRunner().invoke(cli, ["compile", *common(project)])
    assert compiled.exit_code == 0
    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "json"])
    assert listed.exit_code == 0
    assert len(json.loads(listed.output)["data"]) == 14
    cleaned = CliRunner().invoke(cli, ["clean", "--project-dir", str(project), "--output", "json"])
    assert cleaned.exit_code == 0
    assert not (project / "target" / "sst").exists()
