"""SST-PRS020: a key is the deprecated 0.3 spelling of a 1.0 key."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs020_fires(tmp_path: Path) -> None:
    files = metric_file("    expr: COUNT(*)\n    visibility: private_access\n")
    [diagnostic] = found(SmallProject(tmp_path, files=files).load(), "SST-PRS020")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:total: 'visibility' is deprecated; use 'access_modifier'"
    assert diagnostic.subject == "metric:total"


def test_sst_prs020_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: COUNT(*)\n    access_modifier: private_access\n")
    assert found(SmallProject(tmp_path, files=files).load(), "SST-PRS020") == []
