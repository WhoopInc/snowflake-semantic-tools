"""SST-VAL219: a column of a dbt model's `meta.sst.distinct_range` does not exist on the model."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.reference_project import edited_manifest, project_copy, reported, set_model_meta


def _reported(tmp_path: Path, end: str) -> list[Diagnostic]:
    bounds = {"start": "effective_start_at", "end": end}
    manifest = edited_manifest(tmp_path, lambda doc: set_model_meta(doc, "pricing_periods", distinct_range=bounds))
    return reported(project_copy(tmp_path / "project"), "SST-VAL219", manifest)


def test_sst_val219_fires(tmp_path: Path) -> None:
    [diagnostic] = _reported(tmp_path, "closes_at")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dbt_model:pricing_periods: meta.sst.distinct_range.end names column 'closes_at', "
        "which does not exist on pricing_periods"
    )
    assert diagnostic.subject == "dbt_model:pricing_periods"


def test_sst_val219_silent(tmp_path: Path) -> None:
    assert _reported(tmp_path, "effective_end_at") == []
