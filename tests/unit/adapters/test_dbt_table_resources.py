"""What changes for a snapshot or seed in the dbt seam: contracts, staleness and enrich selection."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.project import stale_models
from snowflake_semantic_tools.app.enrich import EnrichRequest, select_models, yaml_section
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.validate.dbt_seam import consumed_model_diagnostics


def _table(name: str, resource_type: str, file: str) -> DbtModel:
    return DbtModel(
        unique_id=f"{resource_type}.fixture.{name}",
        name=name,
        relation_name=f"DB.S.{name.upper()}",
        primary_key=(),
        unique_keys=(),
        # An untyped column: a model like this one is SST-DBT024.
        columns=(DbtColumn(name="id", description=None, data_type=None, column_type=None),),
        original_file_path=file,
        package_name="fixture",
        checksum="0" * 64,
        test_count=1,
        resource_type=resource_type,
    )


CATALOG = DbtCatalog(
    "v12",
    "2.0.8",
    "fixture",
    (
        _table("orders", "model", "models/orders.sql"),
        _table("history", "snapshot", "snapshots/history.yml"),
        _table("raw", "seed", "seeds/raw.csv"),
    ),
)


def test_only_a_model_is_held_to_a_contract() -> None:
    found = consumed_model_diagnostics(CATALOG, ("orders", "history", "raw"))
    assert [(item.code, item.context["model"]) for item in found] == [("SST-DBT024", "orders")]


def test_only_a_model_file_is_compared_for_staleness(tmp_path: Path) -> None:
    for model in CATALOG.models:
        path = tmp_path / str(model.original_file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("changed since the manifest\n", encoding="utf-8")
    roots = ("models", "snapshots", "seeds")
    assert [item.context["value"] for item in stale_models(tmp_path, CATALOG, roots)] == ["models/orders.sql"]


def test_enrich_takes_a_snapshot_or_seed_only_when_a_name_or_path_asks_for_it() -> None:
    def chosen(request: EnrichRequest) -> list[str]:
        return [model.name for model in select_models(CATALOG, request)[0]]

    assert chosen(EnrichRequest()) == ["orders"]
    assert chosen(EnrichRequest(selected=("history",))) == ["history"]
    assert chosen(EnrichRequest(paths=("seeds",))) == ["raw"]
    assert [yaml_section(model) for model in CATALOG.models] == ["models", "snapshots", "seeds"]
    assert yaml_section(replace(CATALOG.models[0], resource_type="model")) == "models"
