"""SST-DBT020: `dbt --version` names no installation SST can identify."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt020_fires(tmp_path: Path) -> None:
    dbt = writing_dbt(tmp_path)
    dbt.version = CompletedRun(0, "dbt-fusion 2.0.0-beta\n", "")
    [diagnostic] = parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=dbt)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT020", Severity.WARNING)
    assert diagnostic.message == "dbt type detection returned 'dbt-fusion 2.0.0-beta'"
    assert diagnostic.subject is None
    # A dbt that runs and exits non-zero names no installation either; it is not SST-DBT027.
    broken = writing_dbt(tmp_path)
    broken.version = CompletedRun(1, "", "dbt: broken install\n")
    [failing] = parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=broken)
    assert failing.message == "dbt type detection returned 'dbt: broken install' (exit 1)"


def test_sst_dbt020_silent(tmp_path: Path) -> None:
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path)) == ()
    # dbt absent from PATH is SST-DBT027 alone: no exit status, so no installation to read.
    absent = refused(tmp_path, FakeDbt(version=FileNotFoundError(2, "No such file or directory")))
    assert absent.code == "SST-DBT027"
