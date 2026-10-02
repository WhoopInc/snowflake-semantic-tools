"""SST-PRS029: a metric's `synonyms` is not a list of strings."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs029_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    synonyms: total\n")).load(), "SST-PRS029"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: synonyms must be a list of strings, found str"
    assert diagnostic.subject == "metric:total"


def test_sst_prs029_silent(tmp_path: Path) -> None:
    assert (
        found(
            SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    synonyms: [all]\n")).load(), "SST-PRS029"
        )
        == []
    )
