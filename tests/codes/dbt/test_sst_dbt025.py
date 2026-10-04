"""SST-DBT025: every semantic load summarises what it read across the dbt seam."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.validate.dbt_seam import seam_summary
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject


def test_sst_dbt025_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(SmallProject(tmp_path).load().diagnostics, "SST-DBT025")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "1 models read, 0 sources read, 1 refs resolved, 1 columns checked"
    assert diagnostic.subject is None


def test_sst_dbt025_silent() -> None:
    # Nothing consumed: the summary still comes once, and counts no column.
    summary = seam_summary(DbtCatalog("v12", None, None, ()), (), 0)
    assert summary.message == "0 models read, 0 sources read, 0 refs resolved, 0 columns checked"
