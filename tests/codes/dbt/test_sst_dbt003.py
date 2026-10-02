"""SST-DBT003: a column's `meta.sst.column_type` is not a role SST knows."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest, model_node


def _column(**sst: str) -> dict[str, object]:
    return {"name": "products_id", "description": "The key.", "data_type": "VARCHAR", "meta": {"sst": sst}}


def test_sst_dbt003_fires(tmp_path: Path) -> None:
    node = model_node("products", columns={"products_id": _column(column_type="measure")})
    [diagnostic] = found(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load(), "SST-DBT003"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'products.products_id': meta.sst role 'measure' is not a known role"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt003_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT003") == []
