"""SST-DBT029: `dbt parse` exits zero and the manifest is not where dbt_project.yml says."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import parse_project
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


def test_sst_dbt029_fires(tmp_path: Path) -> None:
    diagnostic = _refused(tmp_path, FakeDbt())
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT029", Severity.ERROR)
    assert diagnostic.message == "dbt parse exited 0 and target/manifest.json was not written"


def test_sst_dbt029_silent(tmp_path: Path) -> None:
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=_written(tmp_path)) == ()
