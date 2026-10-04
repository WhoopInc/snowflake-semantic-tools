"""SST-DBT029: `dbt parse` exits zero and the manifest is not where dbt_project.yml says."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt029_fires(tmp_path: Path) -> None:
    diagnostic = refused(tmp_path, FakeDbt())
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT029", Severity.ERROR)
    assert diagnostic.message == "dbt parse exited 0 and target/manifest.json was not written"


def test_sst_dbt029_silent(tmp_path: Path) -> None:
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path)) == ()
