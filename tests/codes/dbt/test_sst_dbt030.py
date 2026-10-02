"""SST-DBT030: a model mirrors its relation's location into `meta.sst`, which is forbidden."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest, model_node


def test_sst_dbt030_fires(tmp_path: Path) -> None:
    node = model_node("products", meta={"primary_key": ["products_id"], "schema": "SCH"})
    [diagnostic] = found(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load(), "SST-DBT030"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'products': meta.sst.schema is forbidden -- delete it"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt030_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT030") == []
