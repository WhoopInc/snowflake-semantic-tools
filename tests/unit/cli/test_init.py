"""`sst init`, beside the JSON of the other small commands: `debug`, `list`, and `clean`."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def test_init_debug_clean_and_list_json(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    refused = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--output", "json"])
    assert refused.exit_code == 4
    assert [item["code"] for item in json.loads(refused.output)["diagnostics"]] == ["SST-CFG046"]
    root.mkdir()
    (root / "dbt_project.yml").write_text("name: empty\nprofile: empty\n", encoding="utf-8")
    check = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--check-only", "--output", "json"])
    assert (check.exit_code, json.loads(check.output)["data"]["status"]) == (1, "incomplete")
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--skip-prompts", "--output", "json"])
    assert initialized.exit_code == 0
    assert json.loads(initialized.output)["data"]["created"] == ["sst_config.yml", "semantic_models/semantic_views"]
    assert (root / "semantic_models" / "semantic_views").is_dir()
    assert "snowflake_syntax_check: true" in (root / "sst_config.yml").read_text(encoding="utf-8")
    complete = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--check-only", "--output", "json"])
    assert (complete.exit_code, json.loads(complete.output)["data"]["status"]) == (0, "complete")
    again = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--output", "json"])
    assert json.loads(again.output)["data"] == {
        "created": [],
        "skipped": ["sst_config.yml", "semantic_models/semantic_views"],
        "status": "scaffolded",
    }

    project = project_copy(tmp_path)
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect", "--output", "json"])
    assert debugged.exit_code == 0
    debug_data = json.loads(debugged.output)["data"]
    assert debug_data["target"]["name"] == "dev" and debug_data["connection"] == {"tested": False}
    # The method is shown, never a credential.
    target = debug_data["target"]
    assert isinstance(target["authentication"], str) and "password" not in set(target) - {"authentication"}

    compiled = CliRunner().invoke(cli, ["compile", *common(project)])
    assert compiled.exit_code == 0
    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "json"])
    assert listed.exit_code == 0
    assert json.loads(listed.output)["data"]["count"] == 14
    cleaned = CliRunner().invoke(cli, ["clean", "--project-dir", str(project), "--output", "json"])
    assert cleaned.exit_code == 0
    assert not (project / "target" / "sst").exists()
