"""SST-DBT028: `dbt parse` exits non-zero; dbt's own output is passed on unchanged."""

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


def test_sst_dbt028_fires(tmp_path: Path) -> None:
    echoed: list[str] = []
    dbt = FakeDbt(parse=CompletedRun(2, "Compilation Error in model orders\n", ""))
    diagnostic = _refused(tmp_path, dbt, echo=echoed.append)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT028", Severity.ERROR)
    assert diagnostic.message == "dbt parse exited 2"
    assert echoed == ["Compilation Error in model orders\n"]


def test_sst_dbt028_silent(tmp_path: Path) -> None:
    echoed: list[str] = []
    parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=_written(tmp_path), echo=echoed.append)
    assert echoed == []
