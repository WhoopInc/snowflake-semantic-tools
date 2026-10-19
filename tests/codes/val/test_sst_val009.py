"""SST-VAL009: a semantic-model file has a tab in its indentation, or not exactly one final newline."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import METRICS, edited, project_copy, reported

ROOT = "snowflake_metrics:\n"


def test_sst_val009_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    path = project / METRICS
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    [diagnostic] = reported(project, "SST-VAL009")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "semantic_models/metrics/metrics.yml is not canonically formatted"


def test_sst_val009_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, METRICS, ROOT, ROOT), "SST-VAL009") == []
    # Trailing whitespace is SST-LOD012's alone, so one stray space is reported once.
    assert reported(edited(tmp_path / "space", METRICS, ROOT, "snowflake_metrics:   \n"), "SST-VAL009") == []
