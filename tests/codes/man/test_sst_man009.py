"""SST-MAN009: the project changed after `sst compile`, so plan, apply, and the suites refuse to start."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.app.manifest import manifest_for, stale_inputs
from snowflake_semantic_tools.domain.state import Manifest
from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.compile_builders import compiled as compiled_result
from tests.helpers.project_inputs import EMPTY_SOURCES
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
    assert diagnostic["message"] == (
        f"compiled SST manifest is stale ({_VIEW} changed); run sst compile before plan or apply"
    )
    # Found before the project compiled: no dbt run, no Snowflake session.
    timings = envelope["invocation"]["timings"]  # type: ignore[index]
    assert (timings["dbt_runs"], timings["snowflake"]["connects"]) == (0, 0)
    assert envelope["summary"] == {**envelope["summary"], "error": 1}  # type: ignore[dict-item]


def test_sst_man009_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    exit_code, envelope = _plan(monkeypatch, project)

    assert exit_code in (0, 2) and "SST-MAN009" not in json.dumps(envelope)


def test_sst_man009_names_the_first_input_that_changed() -> None:
    sources = dataclasses.replace(EMPTY_SOURCES, config_checksum="c", file_checksums={"b.yml": "1", "d.yml": "2"})
    compiled = manifest_for(compiled_result(), sources)
    files = {"b.yml": "1", "d.yml": "2"}

    def reason(config: str = "c", found: dict[str, str] = files, manifest: Manifest = compiled) -> str | None:
        stale = stale_inputs(manifest, config_checksum=config, file_checksums=found, before="x")
        return None if stale is None else str(stale.context["reason"])

    assert reason() is None
    assert reason(config="other") == "sst_config.yml changed"
    assert reason(found={**files, "a.yml": "0", "d.yml": "9"}) == "a.yml was added"
    assert reason(found={"b.yml": "1", "d.yml": "9"}) == "d.yml changed"
    assert reason(found={"d.yml": "2"}) == "b.yml was removed"
    older = dataclasses.replace(compiled, generator={"name": "sst", "version": "0.9.0"})
    assert str(reason(manifest=older)).startswith("SST 0.9.0 compiled it")
