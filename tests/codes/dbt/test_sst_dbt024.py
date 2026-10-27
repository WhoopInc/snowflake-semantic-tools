"""SST-DBT024: a model a view consumes enforces no dbt contract and leaves a column's type to inference."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, manifest, model_node


def _uncontracted(column: dict[str, Any]) -> dict[str, Any]:
    node = model_node("products", columns={"products_id": {"name": "products_id", "description": "The key.", **column}})
    node["config"]["contract"] = {"enforced": False}
    return node


def test_sst_dbt024_fires(tmp_path: Path) -> None:
    node = _uncontracted({"meta": {"sst": {"column_type": "dimension"}}})
    [diagnostic] = coded(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load().diagnostics, "SST-DBT024"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "model 'products' has no contract"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt024_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path).load().diagnostics, "SST-DBT024") == []


def test_sst_dbt024_silent_for_an_uncontracted_model_that_types_every_column_in_meta(tmp_path: Path) -> None:
    # Deliberately no contract: `meta.sst.data_type` is how most production columns are typed.
    node = _uncontracted({"meta": {"sst": {"column_type": "dimension", "data_type": "VARCHAR"}}})
    project = SmallProject(tmp_path, document=manifest({"model.fixture.products": node}))
    assert coded(project.load().diagnostics, "SST-DBT024") == []
