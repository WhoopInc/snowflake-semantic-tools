"""`sst list`: compiled artifacts, and the error without a compiled manifest."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli

from .helpers import common, project_copy


def test_list_without_manifest_and_failed_compile_json(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    missing = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "json"])
    assert missing.exit_code == 4
    payload = json.loads(missing.output)
    assert payload["exit_code"] == 4

    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("{{ ref('products') }}", "{{ ref('missing') }}", 1), encoding="utf-8"
    )
    failed = CliRunner().invoke(cli, ["compile", *common(project), "--output", "json"])
    assert failed.exit_code == 1
    assert json.loads(failed.output)["status"] == "error"
