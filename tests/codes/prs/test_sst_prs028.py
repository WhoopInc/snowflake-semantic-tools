"""SST-PRS028: a dbt model's `distinct_range` lacks a start or an end column."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.reference_project import edited_manifest, project_copy, reported, set_model_meta


def _reported(tmp_path: Path, distinct_range: object) -> list[Diagnostic]:
    manifest = edited_manifest(
        tmp_path, lambda doc: set_model_meta(doc, "pricing_periods", distinct_range=distinct_range)
    )
    return reported(project_copy(tmp_path / "project"), "SST-PRS028", manifest)


def test_sst_prs028_fires(tmp_path: Path) -> None:
    [diagnostic] = _reported(tmp_path, {"start": "effective_start_at"})
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dbt_model:pricing_periods: constraints block is invalid: "
        "meta.sst.distinct_range needs a start and an end column"
    )
    assert diagnostic.subject == "dbt_model:pricing_periods"


def test_sst_prs028_fires_for_a_range_that_is_not_a_mapping(tmp_path: Path) -> None:
    assert len(_reported(tmp_path, ["effective_start_at", "effective_end_at"])) == 1


def test_sst_prs028_keeps_sst_val206_quiet(tmp_path: Path) -> None:
    manifest = edited_manifest(tmp_path, lambda doc: set_model_meta(doc, "pricing_periods", distinct_range="x"))
    assert reported(project_copy(tmp_path / "project"), "SST-VAL206", manifest) == []


def test_sst_prs028_silent(tmp_path: Path) -> None:
    assert _reported(tmp_path, {"start": "effective_start_at", "end": "effective_end_at"}) == []
