"""SST-DBT020: `dbt --version` names no installation SST can identify."""

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


def test_sst_dbt020_fires(tmp_path: Path) -> None:
    dbt = _written(tmp_path)
    dbt.version = CompletedRun(0, "dbt-fusion 2.0.0-beta\n", "")
    [diagnostic] = parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=dbt)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT020", Severity.WARNING)
    assert diagnostic.message == "dbt type detection returned 'dbt-fusion 2.0.0-beta'"
    assert diagnostic.subject is None
    # A dbt that runs and exits non-zero names no installation either; it is not SST-DBT027.
    broken = _written(tmp_path)
    broken.version = CompletedRun(1, "", "dbt: broken install\n")
    [failing] = parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=broken)
    assert failing.message == "dbt type detection returned 'dbt: broken install' (exit 1)"


def test_sst_dbt020_silent(tmp_path: Path) -> None:
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=_written(tmp_path)) == ()
    # dbt absent from PATH is SST-DBT027 alone: no exit status, so no installation to read.
    absent = _refused(tmp_path, FakeDbt(version=FileNotFoundError(2, "No such file or directory")))
    assert absent.code == "SST-DBT027"
