"""SST-DBT009: a view consumes a model that is disabled or ephemeral, so has no relation."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest, model_node


def test_sst_dbt009_fires(tmp_path: Path) -> None:
    node = model_node("products", relation="", config={"materialized": "ephemeral"})
    [diagnostic] = found(
        SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load(), "SST-DBT009"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'products' is ephemeral and produces no relation"
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_dbt009_fires_for_a_disabled_model(tmp_path: Path) -> None:
    disabled = {"model.fixture.products": [{"resource_type": "model", "name": "products"}]}
    [diagnostic] = found(SmallProject(tmp_path, document=manifest({}, disabled=disabled)).load(), "SST-DBT009")
    assert diagnostic.message == "model 'products' is disabled and produces no relation"


def test_sst_dbt009_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT009") == []
