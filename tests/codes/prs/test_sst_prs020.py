"""SST-PRS020: a key is the deprecated 0.3 spelling of a 1.0 key."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, metric_file


def test_sst_prs020_fires(tmp_path: Path) -> None:
    # `visibility` is still honoured, and reported as SST-VAL122; this spelling is not read.
    files = metric_file("    expr: COUNT(*)\n    non_additive_by: []\n")
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS020")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'non_additive_by' is deprecated; use 'non_additive_dimensions'"
    assert diagnostic.subject == "metric:total"


def test_sst_prs020_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: COUNT(*)\n    non_additive_dimensions: []\n")
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS020") == []
