"""SST-PRS022: an unknown field is one or two edits from a field SST models, so it is a typo."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs022_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    synonym: [all]\n")).load(), "SST-PRS022"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: unknown field 'synonym'; did you mean 'synonyms'?"
    assert diagnostic.subject == "metric:total"


def test_sst_prs022_silent(tmp_path: Path) -> None:
    assert (
        found(
            SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    synonyms: [all]\n")).load(), "SST-PRS022"
        )
        == []
    )
