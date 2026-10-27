"""SST-ASK011: `ask.theme` names a theme the full-screen conversation does not have."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.cli.tui.ask_app import AskApp
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def _opened(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, theme: str) -> tuple[int, list[str], list[str]]:
    themes: list[str] = []
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.ask._interactive", lambda: True)
    monkeypatch.setattr(AskApp, "run", lambda self: themes.append(self._theme_name))
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8") + f"\nask:\n  theme: {theme}\n", encoding="utf-8")
    result = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["ask", *common(project), "--target", "dev", "-o", "json"]
    )
    codes = [item["code"] for item in json.loads(result.output)["diagnostics"]]
    return result.exit_code, codes, themes


def test_sst_ask011_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exit_code, codes, themes = _opened(tmp_path, monkeypatch, "neon")
    assert (exit_code, codes, themes) == (4, ["SST-ASK011"], [])


def test_sst_ask011_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exit_code, codes, themes = _opened(tmp_path, monkeypatch, "nord")
    assert (exit_code, codes, themes) == (0, [], ["nord"])
