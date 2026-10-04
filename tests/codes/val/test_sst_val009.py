"""SST-VAL009: a semantic-model file is not canonically formatted."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import METRICS, edited, reported

ROOT = "snowflake_metrics:\n"


def test_sst_val009_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(edited(tmp_path, METRICS, ROOT, "snowflake_metrics:   \n"), "SST-VAL009")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_models/metrics/metrics.yml is not canonically formatted"


def test_sst_val009_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, METRICS, ROOT, ROOT), "SST-VAL009") == []
