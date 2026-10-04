"""SST-VAL016: an Analyst tool names a semantic view the project declares and does not publish."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.reference_project import VIEWS, compiled_diagnostics, edited

ENABLED = "  - name: jaffle_sales\n"


def test_sst_val016_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, VIEWS, ENABLED, ENABLED + "    enabled: false\n")
    project_views = project / VIEWS
    project_views.write_text(project_views.read_text().replace("    enabled: true\n", "", 1))
    [found] = compiled_diagnostics(project, "SST-VAL016")
    assert found["severity"] == "error"
    assert found["message"] == (
        "agent 'jaffle_analytics_agent' references 'semantic_view:jaffle_sales', "
        "which is neither published nor declared"
    )
    assert compiled_diagnostics(project, "SST-REF011") == []


def test_sst_val016_silent(tmp_path: Path) -> None:
    assert compiled_diagnostics(edited(tmp_path, VIEWS, ENABLED, ENABLED), "SST-VAL016") == []
