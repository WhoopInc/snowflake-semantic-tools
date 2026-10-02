"""SST-DBT027: dbt cannot be started at all, so there is no exit status to read."""

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


def test_sst_dbt027_fires(tmp_path: Path) -> None:
    diagnostic = _refused(tmp_path, FakeDbt(version=FileNotFoundError(2, "No such file or directory")))
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT027", Severity.ERROR)
    assert diagnostic.message == "dbt could not be executed: dbt: No such file or directory"
    failing = _refused(tmp_path, FakeDbt(version=CompletedRun(1, "", "dbt: broken install\n")))
    assert failing.message == "dbt could not be executed: dbt --version failed: dbt: broken install"


def test_sst_dbt027_silent(tmp_path: Path) -> None:
    dbt = _written(tmp_path)
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=dbt) == ()
    assert dbt.runs[1] == (
        "dbt",
        "parse",
        "--project-dir",
        str(tmp_path),
        "--profiles-dir",
        str(tmp_path),
        "--target",
        "dev",
    )
