"""SST-DBT021: `defer.auto_compile` is set under the dbt Cloud CLI, which cannot parse another target."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt021_fires(tmp_path: Path) -> None:
    dbt = FakeDbt(version=CompletedRun(0, "dbt Cloud CLI - 0.38.6\n", ""))
    diagnostic = refused(tmp_path, dbt, auto_compile=True)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT021", Severity.ERROR)
    assert diagnostic.message == "defer.auto_compile is true under the dbt Cloud CLI"
    assert [run[1] for run in dbt.runs] == ["--version"]


def test_sst_dbt021_silent(tmp_path: Path) -> None:
    dbt = writing_dbt(tmp_path)
    dbt.version = CompletedRun(0, "dbt Cloud CLI - 0.38.6\n", "")
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=dbt) == ()
