"""SST-DBT032: a model writes `meta.sst.primary_key` or `unique_keys` in the 0.3 form."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, manifest, model_node


def test_sst_dbt032_fires(tmp_path: Path) -> None:
    node = model_node("products", meta={"primary_key": "products_id"})
    [diagnostic] = coded(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load().diagnostics, "SST-DBT032"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'products': meta.sst.primary_key is written in the 0.3 form"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt032_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path).load().diagnostics, "SST-DBT032") == []
