"""SST-PRS008: a table-scoped metric and a derived metric share a name."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, metric_file

DERIVED = "  - name: {name}\n    description: Derived.\n    derived: true\n    expr: \"{{{{ metric('x') }}}}\"\n"


def test_sst_prs008_fires(tmp_path: Path) -> None:
    files = metric_file("    expr: COUNT(*)\n" + DERIVED.format(name="total"))
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric 'total' collides with a derived metric name"
    assert diagnostic.subject == "metric:total"


def test_sst_prs008_silent(tmp_path: Path) -> None:
    files = metric_file("    expr: COUNT(*)\n" + DERIVED.format(name="total_share"))
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS008") == []
