"""SST-DBT026: `packages.yml` declares packages that are not installed, so dbt is not run."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt026_fires(tmp_path: Path) -> None:
    (tmp_path / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n")
    dbt = FakeDbt()
    diagnostic = refused(tmp_path, dbt)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT026", Severity.ERROR)
    assert diagnostic.message == "1 package(s) in packages.yml are not installed"
    assert dbt.runs == []


def test_sst_dbt026_silent(tmp_path: Path) -> None:
    (tmp_path / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n")
    (tmp_path / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path)) == ()
