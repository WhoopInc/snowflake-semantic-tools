"""SST-PRS123: a view fails to build for a reason with no code of its own."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import VIEWS, SmallProject, view_file

VARIABLE = "    variables:\n      - name: tax_inclusive\n{data_type}        default_value: true\n"


def test_sst_prs123_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files=view_file(VARIABLE.format(data_type=""))).load()
    [diagnostic] = coded(project.diagnostics, "SST-PRS123")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:catalog: variable {'name': 'tax_inclusive', 'default_value': True} "
        "must set name, data_type and default_value"
    )
    assert diagnostic.subject == "semantic_view:catalog"
    assert diagnostic.origin is not None and diagnostic.origin.file == VIEWS


def test_sst_prs123_silent(tmp_path: Path) -> None:
    files = view_file(VARIABLE.format(data_type="        data_type: BOOLEAN\n"))
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS123") == []
