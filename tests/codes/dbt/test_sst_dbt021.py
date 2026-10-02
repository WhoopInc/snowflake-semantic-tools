"""SST-DBT021: `defer.auto_compile` is set under the dbt Cloud CLI, which cannot parse another target."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, parse_project
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.seam_projects import FakeDbt, manifest


def _refused(
    tmp_path: Path, dbt: FakeDbt, *, auto_compile: bool = False, echo: Callable[[str], object] = print
) -> Diagnostic:
    with pytest.raises(ProjectError) as raised:
        manifest_path = tmp_path / "target" / "manifest.json"
        parse_project(tmp_path, "dev", manifest_path, runner=dbt, auto_compile=auto_compile, echo=echo)
    [diagnostic] = raised.value.diagnostics
    return diagnostic


def _written(tmp_path: Path) -> FakeDbt:
    return FakeDbt(writes=(tmp_path / "target" / "manifest.json", manifest()))


def test_sst_dbt021_fires(tmp_path: Path) -> None:
    dbt = FakeDbt(version=CompletedRun(0, "dbt Cloud CLI - 0.38.6\n", ""))
    diagnostic = _refused(tmp_path, dbt, auto_compile=True)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT021", Severity.ERROR)
    assert diagnostic.message == "defer.auto_compile is true under the dbt Cloud CLI"
    assert [run[1] for run in dbt.runs] == ["--version"]


def test_sst_dbt021_silent(tmp_path: Path) -> None:
    dbt = _written(tmp_path)
    dbt.version = CompletedRun(0, "dbt Cloud CLI - 0.38.6\n", "")
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=dbt) == ()
