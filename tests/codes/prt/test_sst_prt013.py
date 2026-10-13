"""SST-PRT013: a skill folder or file the agent kit would write is in the way, and SST did not write it."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.adapters.agent_kit import read_kit
from snowflake_semantic_tools.cli.main import cli


def _install(project: Path) -> tuple[int, list[dict[str, str]]]:
    result = CliRunner().invoke(
        cli, ["-o", "json", "agent-kit", "install", "--agent", "claude", "--project-dir", str(project)]
    )
    return result.exit_code, json.loads(result.stdout)["diagnostics"]


def test_sst_prt013_fires(tmp_path: Path) -> None:
    folder = tmp_path / ".claude/skills/sst-core"
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text("my own skill\n", encoding="utf-8")
    code, [diagnostic] = _install(tmp_path)
    assert (code, diagnostic["code"], diagnostic["severity"]) == (1, "SST-PRT013", "error")
    assert diagnostic["message"] == ".claude/skills/sst-core: the folder exists and SST did not write it"
    assert (folder / "SKILL.md").read_text(encoding="utf-8") == "my own skill\n"
    assert not (tmp_path / ".sst").exists()


def test_sst_prt013_silent(tmp_path: Path) -> None:
    # A folder that already holds the kit's files exactly, as after a lost lock, is adopted.
    folder = tmp_path / ".claude/skills/sst-core"
    for path, data in read_kit(__version__)["sst-core"].items():
        (folder / path).parent.mkdir(parents=True, exist_ok=True)
        (folder / path).write_bytes(data)
    code, diagnostics = _install(tmp_path)
    assert (code, diagnostics) == (0, [])
    assert (tmp_path / ".sst/agent-kit.json").is_file()
