"""SST-DBT027: dbt cannot be started at all, so there is no exit status to read."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt027_fires(tmp_path: Path) -> None:
    diagnostic = refused(tmp_path, FakeDbt(version=FileNotFoundError(2, "No such file or directory")))
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT027", Severity.ERROR)
    assert diagnostic.message == "dbt could not be executed: dbt: No such file or directory"


def test_sst_dbt027_silent(tmp_path: Path) -> None:
    broken = writing_dbt(tmp_path)
    broken.version = CompletedRun(1, "", "dbt: broken install\n")
    assert [
        item.code for item in parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=broken)
    ] == ["SST-DBT020"]
    dbt = writing_dbt(tmp_path)
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
