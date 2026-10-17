"""SST-MAN009: the project changed after `sst compile`, so plan, apply, and the suites refuse to start."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

_VIEW = "semantic_models/semantic_views/jaffle_minimal.yml"


def _plan(monkeypatch: pytest.MonkeyPatch, project: Path) -> tuple[int, dict[str, object]]:
    args = ["plan", *common(project), "--target", "dev", "--output", "json"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    return result.exit_code, json.loads(result.output)


def test_sst_man009_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    views = project / _VIEW
    views.write_text(views.read_text(encoding="utf-8").replace("Menu products only", "Edited"), encoding="utf-8")

    exit_code, envelope = _plan(monkeypatch, project)

    diagnostics = envelope["diagnostics"]
    assert isinstance(diagnostics, list)
    [diagnostic] = diagnostics
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (4, "SST-MAN009", "error")
    assert diagnostic["message"] == "compiled SST manifest is stale; run sst compile before plan or apply"
    assert envelope["summary"] == {**envelope["summary"], "error": 1}  # type: ignore[dict-item]


def test_sst_man009_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    exit_code, envelope = _plan(monkeypatch, project)

    assert exit_code in (0, 2) and "SST-MAN009" not in json.dumps(envelope)
