"""The dbt seam rules: what the manifest says about the models the semantic layer consumes."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel, DbtSource
from snowflake_semantic_tools.domain.validate.dbt_seam import (
    collapse_diagnostics,
    consumed_model_diagnostics,
    empty_catalog,
    fan_out_diagnostics,
    seam_summary,
    unreadable_model,
)


def _model(name: str, *, relation: str = "", columns: tuple[DbtColumn, ...] = (), **fields: object) -> DbtModel:
    return DbtModel(
        f"model.p.{name}",
        name,
        relation or f"DB.S.{name.upper()}",
        (),
        (),
        columns,
        **fields,  # type: ignore[arg-type]
    )


def _column(name: str, column_type: str | None = "dimension") -> DbtColumn:
    return DbtColumn(name, None, "TEXT", column_type)


def _catalog(*models: DbtModel, **fields: object) -> DbtCatalog:
    return DbtCatalog("v12", None, "p", models, **fields)  # type: ignore[arg-type]


def test_an_empty_catalog_is_reported_only_when_it_lists_no_model_at_all() -> None:
    assert [item.code for item in empty_catalog(_catalog())] == ["SST-DBT001"]
    assert empty_catalog(_catalog(_model("orders"))) == ()
    assert empty_catalog(_catalog(unreadable_models=MappingProxyType({"stg": "ephemeral"}))) == ()


def test_an_unreadable_model_names_why_and_whom() -> None:
    catalog = _catalog(unreadable_models=MappingProxyType({"stg": "ephemeral"}))
    assert unreadable_model(catalog, "orders") is None
    found = unreadable_model(catalog, "STG")
    assert found is not None and (found.code, found.subject) == ("SST-DBT009", "dbt_model:STG")
    named = unreadable_model(catalog, "stg", subject="semantic_view:v")
    assert named is not None and named.subject == "semantic_view:v"


def test_each_consumed_model_is_checked_for_a_checksum_tests_and_a_contract() -> None:
    bare = _model("orders")
    sound = _model("customers", checksum="abc", test_count=1, has_contract=True)
    unused = _model("unused")
    found = consumed_model_diagnostics(_catalog(bare, sound, unused), ("ORDERS", "customers"))
    assert [(item.code, item.subject) for item in found] == [
        ("SST-DBT015", "dbt_model:orders"),
        ("SST-DBT023", "dbt_model:orders"),
        ("SST-DBT024", "dbt_model:orders"),
    ]


def test_models_that_collapse_to_one_relation_differ_in_a_semantic_column() -> None:
    first = _model("a_orders", relation="DB.S.ORDERS", columns=(_column("id"), _column("note", None)))
    other = _model("b_orders", relation="db.s.orders", columns=(_column("id"), _column("total")))
    same = _model("c_orders", relation="DB.S.ORDERS", columns=(_column("ID"),))
    alone = _model("customers", columns=(_column("id"),))
    found = collapse_diagnostics(_catalog(first, other, same, alone), ("a_orders", "customers"))
    assert [(item.code, item.context["b"], item.context["column"]) for item in found] == [
        ("SST-DBT010", "b_orders", "total")
    ]
    # A relation no consumed model reads is not compared.
    assert collapse_diagnostics(_catalog(first, other), ("customers",)) == ()


def test_a_model_feeding_several_artifacts_is_named_as_dbt_names_it() -> None:
    catalog = _catalog(_model("Orders"))
    feeds = {"semantic_view:a": ("orders", "customers"), "semantic_view:b": ("ORDERS",), "tool:t": ("stg",)}
    found = fan_out_diagnostics(feeds, catalog)
    assert [(item.code, item.context["model"], item.context["count"]) for item in found] == [
        ("SST-DBT016", "Orders", 2)
    ]
    unknown = fan_out_diagnostics({"semantic_view:a": ("stg",), "semantic_view:b": ("stg",)}, catalog)
    assert [item.subject for item in unknown] == ["dbt_model:stg"]


def test_the_seam_summary_counts_models_sources_refs_and_consumed_columns() -> None:
    catalog = _catalog(
        _model("orders", columns=(_column("id"), _column("total"))),
        _model("unused", columns=(_column("id"),)),
        sources=(DbtSource("source.p.raw.events", "raw", "events", "DB.RAW.EVENTS"),),
    )
    summary = seam_summary(catalog, ("ORDERS",), 3)
    assert summary.code == "SST-DBT025"
    assert summary.message == "2 models read, 1 sources read, 3 refs resolved, 2 columns checked"
