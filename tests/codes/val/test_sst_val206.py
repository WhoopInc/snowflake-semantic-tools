"""SST-VAL206: a range relationship's target dbt model declares no distinct_range."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import DBT_MANIFEST, edited_manifest, project_copy, reported, set_model_meta


def test_sst_val206_fires(tmp_path: Path) -> None:
    manifest = edited_manifest(tmp_path, lambda doc: set_model_meta(doc, "pricing_periods", distinct_range=None))
    [diagnostic] = reported(project_copy(tmp_path / "project"), "SST-VAL206", manifest)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "relationship 'orders_to_pricing_periods' is a range join and 'pricing_periods' declares no distinct_range"
    )
    assert diagnostic.subject == "semantic_view:jaffle_menu"


def test_sst_val206_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL206", DBT_MANIFEST) == []
