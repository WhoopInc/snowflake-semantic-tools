"""SST-DBT026: `packages.yml` declares packages that are not installed, so dbt is not run."""

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


def test_sst_dbt026_fires(tmp_path: Path) -> None:
    (tmp_path / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n")
    dbt = FakeDbt()
    diagnostic = _refused(tmp_path, dbt)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT026", Severity.ERROR)
    assert diagnostic.message == "1 package(s) in packages.yml are not installed"
    assert dbt.runs == []


def test_sst_dbt026_silent(tmp_path: Path) -> None:
    (tmp_path / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n")
    (tmp_path / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=_written(tmp_path)) == ()
