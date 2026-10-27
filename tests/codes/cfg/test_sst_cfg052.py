"""SST-CFG052: the agent kit's folder overlaps a directory SST publishes from."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli


def _install(project: Path) -> tuple[int, list[dict[str, str]]]:
    result = CliRunner().invoke(
        cli, ["-o", "json", "agent-kit", "install", "--agent", "cortex", "--project-dir", str(project)]
    )
    return result.exit_code, json.loads(result.stdout)["diagnostics"]


def test_sst_cfg052_fires(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("project:\n  skills_dir: .cortex/skills/published\n", encoding="utf-8")
    code, [diagnostic] = _install(tmp_path)
    assert (code, diagnostic["code"], diagnostic["severity"]) == (4, "SST-CFG052", "error")
    assert diagnostic["message"] == (
        ".cortex/skills overlaps project.skills_dir (.cortex/skills/published), a directory SST publishes from"
    )
    assert not (tmp_path / ".cortex").exists()


def test_sst_cfg052_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("project:\n  skills_dir: skills\n", encoding="utf-8")
    code, diagnostics = _install(tmp_path)
    assert (code, diagnostics) == (0, [])
    assert (tmp_path / ".cortex/skills/sst-core/SKILL.md").is_file()
