"""SST-DBT004: a column's `meta.sst.data_type` disagrees with the type dbt records."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest, model_node


def _column(**sst: str) -> dict[str, object]:
    return {"name": "products_id", "description": "The key.", "data_type": "VARCHAR", "meta": {"sst": sst}}


def test_sst_dbt004_fires(tmp_path: Path) -> None:
    node = model_node("products", columns={"products_id": _column(column_type="dimension", data_type="NUMBER")})
    [diagnostic] = found(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load(), "SST-DBT004"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "model 'products': column 'products_id' is VARCHAR in dbt and NUMBER in the semantic layer"
    )
    assert diagnostic.subject == "dbt_column:products.products_id"


def test_sst_dbt004_silent(tmp_path: Path) -> None:
    # The same type written another way agrees.
    node = model_node("products", columns={"products_id": _column(column_type="dimension", data_type="varchar")})
    assert found(SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load(), "SST-DBT004") == []
