"""SST-DBT024: a model a view consumes enforces no dbt contract."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest, model_node


def test_sst_dbt024_fires(tmp_path: Path) -> None:
    node = model_node("products")
    node["config"]["contract"] = {"enforced": False}
    [diagnostic] = found(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load(), "SST-DBT024"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "model 'products' has no contract"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt024_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT024") == []
