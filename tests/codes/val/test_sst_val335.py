"""SST-VAL335: a fact declares is_enum: true."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.reference_project import edited_manifest, project_copy, reported, set_column_meta


def _enum(tmp_path: Path, column: str) -> Path:
    return edited_manifest(
        tmp_path,
        lambda document: set_column_meta(document, "orders", column, is_enum=True, sample_values=["1", "2"]),
    )


def test_sst_val335_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    manifest = _enum(tmp_path, "order_total")
    [diagnostic] = reported(project, "SST-VAL335", manifest)
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "dbt_model:orders: fact 'order_total' declares is_enum; only a dimension can be an enum"
    )
    assert diagnostic.subject == "dbt_column:orders.order_total"
    assert ERROR_REGISTRY["SST-VAL335"].always_error


def test_sst_val335_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL335", _enum(tmp_path, "order_state")) == []
