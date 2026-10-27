"""SST-DBT010: two models resolve to one relation and declare different semantic columns."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, manifest, model_node

OTHER = {"other": {"name": "other", "data_type": "VARCHAR", "meta": {"sst": {"column_type": "dimension"}}}}


def test_sst_dbt010_fires(tmp_path: Path) -> None:
    nodes = {
        "model.fixture.products": model_node("products"),
        "model.other.products_copy": model_node("products_copy", "db.sch.products", columns=OTHER),
    }
    [diagnostic] = coded(SmallProject(tmp_path, document=manifest(nodes)).load().diagnostics, "SST-DBT010")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "models products and products_copy collapse to 'DB.SCH.PRODUCTS' and differ in other"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt010_silent(tmp_path: Path) -> None:
    # Two models on one relation that agree on their columns are not a hazard.
    nodes = {
        "model.fixture.products": model_node("products"),
        "model.other.products_copy": model_node("products_copy", "db.sch.products", columns=None),
    }
    nodes["model.other.products_copy"]["columns"] = nodes["model.fixture.products"]["columns"]
    assert coded(SmallProject(tmp_path, document=manifest(nodes)).load().diagnostics, "SST-DBT010") == []
