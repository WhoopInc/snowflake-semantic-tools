"""Strict mode promotes the warnings planning reports, as it does validation's, in plan and apply.

A plan outside a git checkout warns that each eval dataset it mints carries no commit
(SST-VAL714). That warning is the plan's own, not validation's, and strict mode promotes it too.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

# The fixture's deliberate validation warnings, made info so that only the plan's own warns.
_QUIET = ("SST-RND013", "SST-VAL528", "SST-RND010", "SST-CFG018")


def _project(tmp_path: Path, *, strict: bool) -> Path:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    text = config.read_text(encoding="utf-8")
    quiet = "".join(f"    {code}: info\n" for code in _QUIET)
    text = text.replace("    SST-VAL531: warning\n", f"    SST-VAL531: warning\n{quiet}")
    config.write_text(text.replace("strict: false", f"strict: {str(strict).lower()}"), encoding="utf-8")
    return project


def _run(monkeypatch: pytest.MonkeyPatch, project: Path, command: str) -> tuple[int, dict[str, object]]:
    flags = ["--no-plan-out"] if command == "plan" else ["--yes"]
    args = [command, *common(project), "--target", "dev", "--no-snowflake-syntax-check", "--output", "json", *flags]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    return result.exit_code, json.loads(result.output)


def _warned(envelope: dict[str, object]) -> list[tuple[str, str, str | None]]:
    diagnostics = envelope["diagnostics"]
    assert isinstance(diagnostics, list)
    return [
        (item["code"], item["severity"], item["promoted_from"]) for item in diagnostics if item["severity"] != "info"
    ]


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_strict_promotes_a_warning_the_plan_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    exit_code, envelope = _run(monkeypatch, _project(tmp_path, strict=True), command)

    assert exit_code == 1
    assert _warned(envelope) == [("SST-VAL714", "error", "warning")]
    summary = envelope["summary"]
    assert isinstance(summary, dict) and (summary["promoted"], summary["error"]) == (1, 1)
    if command == "apply":
        data = envelope["data"]
        assert isinstance(data, dict) and data["applied"] == []


def test_without_strict_the_plan_reports_it_as_a_warning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exit_code, envelope = _run(monkeypatch, _project(tmp_path, strict=False), "plan")

    assert exit_code == 2
    assert _warned(envelope) == [("SST-VAL714", "warning", None)]
    summary = envelope["summary"]
    assert isinstance(summary, dict) and summary["promoted"] == 0
