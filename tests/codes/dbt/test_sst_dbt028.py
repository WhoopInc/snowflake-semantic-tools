"""SST-DBT028: `dbt parse` exits non-zero; dbt's own output is passed on unchanged."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt028_fires(tmp_path: Path) -> None:
    echoed: list[str] = []
    dbt = FakeDbt(parse=CompletedRun(2, "Compilation Error in model orders\n", ""))
    diagnostic = refused(tmp_path, dbt, echo=echoed.append)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT028", Severity.ERROR)
    assert diagnostic.message == "dbt parse exited 2"
    assert echoed == ["Compilation Error in model orders\n"]


def test_sst_dbt028_silent(tmp_path: Path) -> None:
    echoed: list[str] = []
    parse_project(
        tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path), echo=echoed.append
    )
    assert echoed == []
