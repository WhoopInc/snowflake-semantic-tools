"""SST-DBT009: a member consumes a model that is disabled or ephemeral, so has no relation.

A view's own table naming such a model is SST-VAL303's; this code names the other consumers.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest, metric_file, model_node

METRIC = metric_file("    expr: COUNT(*)\n")


def test_sst_dbt009_fires(tmp_path: Path) -> None:
    node = model_node("products", relation="", config={"materialized": "ephemeral"})
    project = SmallProject(tmp_path, files=METRIC, document=manifest({"model.fixture.products": node})).load()
    [diagnostic] = found(project, "SST-DBT009")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'products' is ephemeral and produces no relation"
    assert diagnostic.subject == "metric:total"
    assert [item.subject for item in found(project, "SST-VAL303")] == ["semantic_view:catalog"]


def test_sst_dbt009_fires_for_a_disabled_model(tmp_path: Path) -> None:
    disabled = {"model.fixture.products": [{"resource_type": "model", "name": "products"}]}
    project = SmallProject(tmp_path, files=METRIC, document=manifest({}, disabled=disabled)).load()
    [diagnostic] = found(project, "SST-DBT009")
    assert diagnostic.message == "model 'products' is disabled and produces no relation"


def test_sst_dbt009_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files=METRIC).load(), "SST-DBT009") == []
