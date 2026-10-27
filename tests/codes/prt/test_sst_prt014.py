"""SST-PRT014: a file you changed is kept although the agent kit would change or remove it."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli

SKILL = Path(".cortex/skills/sst-core/SKILL.md")


def _kit(project: Path, *args: str) -> tuple[int, list[dict[str, str]]]:
    result = CliRunner().invoke(cli, ["-o", "json", "agent-kit", *args, "--project-dir", str(project)])
    return result.exit_code, json.loads(result.stdout)["diagnostics"]


def _install_an_older_kit(project: Path) -> None:
    """Install, then record the core skill as written by an older kit, so the kit now changes it."""
    assert _kit(project, "install", "--agent", "cortex")[0] == 0
    lock = project / ".sst/agent-kit.json"
    document = json.loads(lock.read_text(encoding="utf-8"))
    document["agents"]["cortex"]["skills"]["sst-core"]["SKILL.md"] = "sha256:an-older-kit"
    lock.write_text(json.dumps(document), encoding="utf-8")


def test_sst_prt014_fires(tmp_path: Path) -> None:
    _install_an_older_kit(tmp_path)
    (tmp_path / SKILL).write_text("my notes\n", encoding="utf-8")
    code, [diagnostic] = _kit(tmp_path, "upgrade")
    assert (code, diagnostic["code"], diagnostic["severity"]) == (0, "SST-PRT014", "warning")
    assert (
        diagnostic["message"] == f"{SKILL.as_posix()} changed since SST wrote it, so it is kept: the kit changed it too"
    )
    assert (tmp_path / SKILL).read_text(encoding="utf-8") == "my notes\n"


def test_sst_prt014_silent(tmp_path: Path) -> None:
    # With --force the kit's version replaces the edit, and nothing is kept.
    _install_an_older_kit(tmp_path)
    (tmp_path / SKILL).write_text("my notes\n", encoding="utf-8")
    code, diagnostics = _kit(tmp_path, "upgrade", "--force")
    assert (code, diagnostics) == (0, [])
    assert (tmp_path / SKILL).read_text(encoding="utf-8") != "my notes\n"
