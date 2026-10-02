"""`sst migrate refs`: what it rewrites, what it reports, and what it leaves alone."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli


def test_migrate_refs_reports_calls_it_will_not_rewrite(tmp_path: Path) -> None:
    project = tmp_path / "legacy"
    views = project / "semantic_models" / "semantic_views"
    views.mkdir(parents=True)
    (project / "sst_config.yml").write_text(
        "project:\n  semantic_models_dir: semantic_models\nvalidation:\n  snowflake_syntax_check: false\n",
        encoding="utf-8",
    )
    (views / "views.yml").write_text(
        "semantic_views:\n  - name: v\n    tables:\n      - {{ table('orders') }}\n"
        "    description: \"{{ table('orders') }} is quoted prose\"\n",
        encoding="utf-8",
    )
    human = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(project)])
    assert human.exit_code == 1
    assert "would rewrite semantic_models/semantic_views/views.yml: 1 ref" in human.output
    assert "views.yml:5:" in human.output and "unchanged" in human.output
    machine = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(project), "--write", "--output", "json"])
    assert machine.exit_code == 1
    payload = json.loads(machine.output)
    assert payload["status"] == "error" and payload["data"]["written"] is True
    assert payload["data"]["files"][0]["untouched"][0]["line"] == 5
    assert "{{ ref('orders') }}" in (views / "views.yml").read_text(encoding="utf-8")

    clean = tmp_path / "clean"
    (clean / "semantic_models").mkdir(parents=True)
    (clean / "sst_config.yml").write_text(
        "project:\n  semantic_models_dir: semantic_models\nvalidation:\n  snowflake_syntax_check: false\n",
        encoding="utf-8",
    )
    none = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(clean)])
    assert none.exit_code == 0 and "no legacy references found" in none.output
    missing = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(tmp_path)])
    assert missing.exit_code == 4
