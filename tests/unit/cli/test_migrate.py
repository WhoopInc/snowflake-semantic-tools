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


CONFIG = "project:\n  semantic_models_dir: semantic_models\nvalidation:\n  snowflake_syntax_check: false\n"
GROUP = "tools:\n  - group: g\n    immutable: true\n    reference:\n      - name: r\n        type: stage\n"
METRICS = "snowflake_metrics:\n  - name: a\n    expr: x\n  - name: b\n    expr: y\n"


def project(tmp_path: Path, files: dict[str, str]) -> Path:
    root = tmp_path / "split"
    for path, text in {
        "sst_config.yml": CONFIG,
        **files,
    }.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(text, encoding="utf-8")
    return root


def test_migrate_split_reports_writes_and_then_finds_nothing(tmp_path: Path) -> None:
    root = project(
        tmp_path,
        {
            "semantic_models/metrics/metrics.yml": METRICS,
            "tools/g.yml": GROUP,
            "plugins/p/plugin.yml": "name: p\n",
        },
    )
    dry = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(root)])
    assert dry.exit_code == 2
    assert "would split semantic_models/metrics/metrics.yml into 2 files" in dry.output
    assert "would rewrite sst_config.yml:" in dry.output and "-> tools.g" in dry.output
    written = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(root), "--write", "--output", "json"])
    assert written.exit_code == 0
    payload = json.loads(written.output)
    assert payload["data"]["written"] is True and payload["data"]["config"]["path"] == "sst_config.yml"
    assert not (root / "semantic_models" / "metrics" / "metrics.yml").exists()
    assert (root / "semantic_models" / "metrics" / "a.yml").read_text(
        encoding="utf-8"
    ) == "metric:\n  name: a\n  expr: x\n"
    assert (root / "tools" / "g" / "r.yml").is_file()
    assert "  g:\n    +immutable: true\n" in (root / "sst_config.yml").read_text(encoding="utf-8")
    again = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(root)])
    assert again.exit_code == 0 and "nothing to split" in again.output


def test_migrate_split_leaves_a_list_whole_rather_than_overwrite(tmp_path: Path) -> None:
    root = project(
        tmp_path,
        {
            "semantic_models/metrics/metrics.yml": "snowflake_metrics:\n  - name: a\n    expr: x\n",
            "semantic_models/metrics/a.yml": "metric:\n  name: other\n  expr: y\n",
            "semantic_models/filters/filters.yml": "snowflake_filters:\n  - name: filters\n    expr: z\n",
        },
    )
    result = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(root), "--write"])
    assert result.exit_code == 1
    assert (
        "semantic_models/metrics/metrics.yml: left whole: semantic_models/metrics/a.yml already exists" in result.output
    )
    assert (root / "semantic_models" / "metrics" / "metrics.yml").is_file()
    # A member named for its own list's file replaces the list.
    assert (root / "semantic_models" / "filters" / "filters.yml").read_text(encoding="utf-8") == (
        "filter:\n  name: filters\n  expr: z\n"
    )
    configured = project(
        tmp_path / "c", {"semantic_models/x.yml": "a: 1\n", "sst_config.yml": "skills:\n  stage: {+stage: S}\n"}
    )
    unresolved = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(configured)])
    assert unresolved.exit_code == 1 and "left skills.stage unchanged" in unresolved.output


def test_migrate_refs_reads_tools_agents_and_evals_and_leaves_hooks_and_commands(tmp_path: Path) -> None:
    root = project(
        tmp_path,
        {
            "semantic_models/x.yml": "a: 1\n",
            "tools/t.yml": "tool:\n  name: t\n  type: procedure\n  body_file: tools/sql/t.sql\n",
            "agents/a/agent.yml": "name: a\nalias: x\n",
            "agents/a/evals/config.yml": "agent: a\nname: c\n",
            "hooks/h/hook.yml": "script: run.sh\n",
            "commands/c.md": "---\nskill: s\n---\n",
        },
    )
    result = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(root), "--write"])
    assert result.exit_code == 0
    assert (root / "tools" / "t.yml").read_text(encoding="utf-8").endswith("  body: \"{{ file('sql/t.sql') }}\"\n")
    assert (root / "agents" / "a" / "agent.yml").read_text(encoding="utf-8") == "name: a\nconfig:\n  alias: x\n"
    assert (root / "agents" / "a" / "evals" / "config.yml").read_text(encoding="utf-8") == "name: c\n"
    # Hooks and commands are no longer artifacts SST publishes, so their files are left as written.
    assert (root / "hooks" / "h" / "hook.yml").read_text(encoding="utf-8") == "script: run.sh\n"
    assert (root / "commands" / "c.md").read_text(encoding="utf-8") == "---\nskill: s\n---\n"
