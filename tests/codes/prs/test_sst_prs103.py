"""SST-PRS103: a member's closed field, here `access_modifier`, holds a value it does not allow."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, metric_file


def test_sst_prs103_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files=metric_file("    expr: COUNT(*)\n    access_modifier: hidden\n")).load(),
        "SST-PRS103",
    )
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == "metric:total: 'access_modifier' is 'hidden', expected one of private_access, public_access"
    )
    assert diagnostic.subject == "metric:total"


def test_sst_prs103_silent(tmp_path: Path) -> None:
    assert (
        found(
            SmallProject(
                tmp_path, files=metric_file("    expr: COUNT(*)\n    access_modifier: private_access\n")
            ).load(),
            "SST-PRS103",
        )
        == []
    )
