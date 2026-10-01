"""What `sst enrich` writes for one model's columns, and the prompts and views it involves."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.config_schema import EnrichmentConfig
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.enrich import (
    COLUMN_SYNONYMS_SCHEMA,
    TABLE_SYNONYMS_SCHEMA,
    ColumnUpdate,
    Component,
    EnrichOptions,
    ModelEnrichment,
    PromptColumn,
    ViewTable,
    WarehouseColumn,
    avoided_names,
    column_synonyms,
    column_synonyms_prompt,
    enrich_model,
    needs_table_synonyms,
    parse_column_synonyms,
    parse_table_synonyms,
    prompt_columns,
    resolve_options,
    role,
    sample_columns,
    synonym_columns,
    table_synonym_edits,
    table_synonyms_prompt,
    yaml_column_name,
)

C = Component
SETTINGS = EnrichmentConfig(distinct_limit=3, display_limit=2)
W = WarehouseColumn


def _options(*included: Component, forced: tuple[Component, ...] = ()) -> EnrichOptions:
    return resolve_options(frozenset(included), frozenset(forced))


def _column(name: str, *keys: str, **fields: object) -> DbtColumn:
    values: dict[str, object] = {"description": None, "data_type": None, "column_type": None}
    values.update(fields)
    return DbtColumn(name, declared_keys=frozenset(keys), **values)  # type: ignore[arg-type]


def _model(*columns: DbtColumn, primary_key: tuple[str, ...] = (), tests: frozenset[str] = frozenset()) -> DbtModel:
    return DbtModel("model.p.orders", "orders", "DB.SCH.ORDERS", primary_key, (), columns, key_test_columns=tests)


def test_column_names_and_update_summaries() -> None:
    assert yaml_column_name("ORDER_ID") == "order_id"
    assert yaml_column_name("MixedCase") == "MixedCase"
    update = ColumnUpdate("a", (("column_type", "fact"), ("is_enum", True)), added=True)
    enrichment = ModelEnrichment("orders", (update, ColumnUpdate("b", (("is_enum", False),))))
    assert update.keys == ("column_type", "is_enum")
    assert enrichment.added == ("a",)
    assert enrichment.filled(C.ENUMS) == 2 and enrichment.filled(C.COLUMN_TYPES) == 1
    assert enrichment.filled(C.TABLE_SYNONYMS) == 0


def test_a_written_role_holds_unless_forced_and_keys_derive_as_dimensions() -> None:
    model = _model(_column("total", "column_type", column_type="dimension"), tests=frozenset({"id"}))
    total = model.columns[0]
    assert role(model, total, W("TOTAL", "NUMBER"), _options()) == "dimension"
    assert role(model, total, W("TOTAL", "NUMBER"), _options(forced=(C.COLUMN_TYPES,))) == "fact"
    assert role(model, None, W("ID", "NUMBER"), _options()) == "dimension"
    keyed = _model(primary_key=("Order_Id",))
    assert role(keyed, None, W("ORDER_ID", "NUMBER"), _options()) == "dimension"


def test_sampling_reads_only_columns_that_gain_values_or_an_enum_decision() -> None:
    model = _model(
        _column("status", "sample_values", sample_values=("a",)),
        _column("email", pii_tagged=True),
        _column("payload"),
        _column("hidden", excluded=True),
        _column("amount", "sample_values", "column_type", sample_values=("1",), column_type="fact"),
        _column("kind", "sample_values", "is_enum", sample_values=("x",), is_enum=False),
    )
    warehouse = [
        W("STATUS", "TEXT"),
        W("EMAIL", "TEXT"),
        W("PAYLOAD", "VARIANT"),
        W("HIDDEN", "TEXT"),
        W("AMOUNT", "NUMBER"),
        W("KIND", "TEXT"),
        W("NEW", "TEXT"),
        W("new", "TEXT"),
    ]
    assert sample_columns(model, warehouse, _options()) == ()
    names = [column.name for column in sample_columns(model, warehouse, _options(C.SAMPLE_VALUES))]
    assert names == ["NEW"]
    names = [column.name for column in sample_columns(model, warehouse, _options(C.ENUMS))]
    assert names == ["STATUS", "NEW"]
    forced = _options(C.ENUMS, forced=(C.ENUMS, C.SAMPLE_VALUES))
    assert [column.name for column in sample_columns(model, warehouse, forced)] == ["STATUS", "AMOUNT", "KIND", "NEW"]


def test_synonyms_are_asked_for_columns_without_any_unless_forced() -> None:
    model = _model(_column("a", "synonyms", synonyms=("x",)), _column("b", "synonyms"), _column("c", excluded=True))
    warehouse = [W("A", "TEXT"), W("B", "TEXT"), W("C", "TEXT"), W("D", "TEXT")]
    assert synonym_columns(model, warehouse, _options()) == ()
    assert [column.name for column in synonym_columns(model, warehouse, _options(C.COLUMN_SYNONYMS))] == ["B", "D"]
    forced = _options(forced=(C.COLUMN_SYNONYMS,))
    assert [column.name for column in synonym_columns(model, warehouse, forced)] == ["A", "B", "D"]


def test_prompt_columns_show_sampled_or_written_values_but_never_a_pii_columns() -> None:
    model = _model(
        _column("status", description="State", sample_values=("open", "closed")),
        _column("email", sample_values=("a@b.c",), pii_tagged=True),
    )
    columns = [W("STATUS", "VARCHAR"), W("EMAIL", "TEXT"), W("NEW_COL", "NUMBER")]
    described = prompt_columns(model, columns, {"new_col": ["1", "2"]})
    assert described == (
        PromptColumn("status", "TEXT", "State", ("open", "closed")),
        PromptColumn("email", "TEXT", None, ()),
        PromptColumn("new_col", "NUMBER", None, ("1", "2")),
    )


def test_column_synonyms_stay_apart_from_other_columns_names_and_synonyms() -> None:
    model = _model(_column("a"), _column("b", synonyms=("bee",)))
    warehouse = [W("A", "TEXT"), W("B", "TEXT"), W("C", "TEXT")]
    proposals = {"a": ["bee", "c", "alpha", "shared"], "c": ["shared", "gamma"], "zzz": ["unused"]}
    assert column_synonyms(model, warehouse, proposals, limit=4) == {"a": ("alpha", "shared"), "c": ("gamma",)}
    assert column_synonyms(model, warehouse, {"a": ["b"]}, limit=4) == {}


def test_types_fill_missing_values_keep_written_ones_and_report_a_disagreeing_type() -> None:
    model = _model(
        _column("id", "data_type", data_type="number"),
        _column("note", "data_type", "column_type", data_type="NUMBER", column_type="dimension"),
        _column("code", data_type="TEXT", native_data_type="TEXT"),
        _column("amount", "data_type", data_type="varchar"),
        primary_key=("id",),
    )
    warehouse = [W("ID", "NUMBER"), W("NOTE", "TEXT"), W("CODE", "NUMBER"), W("AMOUNT", "TEXT"), W("ADDED", "FLOAT")]
    result = enrich_model(model, warehouse, _options(), SETTINGS, samples={}, synonyms={})
    assert result.updates == (
        ColumnUpdate("id", (("column_type", "dimension"),)),
        ColumnUpdate("code", (("column_type", "fact"),)),
        ColumnUpdate("amount", (("column_type", "dimension"),)),
        ColumnUpdate("added", (("column_type", "fact"), ("data_type", "FLOAT")), added=True),
    )
    assert [(item.code, item.subject) for item in result.diagnostics] == [("SST-VAL327", "dbt_column:orders.note")]
    assert "declares data_type NUMBER, and the relation has TEXT" in result.diagnostics[0].message
    forced = enrich_model(
        model, warehouse, _options(forced=(C.DATA_TYPES, C.COLUMN_TYPES)), SETTINGS, samples={}, synonyms={}
    )
    assert [(update.name, update.values) for update in forced.updates][:3] == [
        ("id", (("column_type", "dimension"), ("data_type", "NUMBER"))),
        ("note", (("data_type", "TEXT"),)),
        ("code", (("column_type", "fact"),)),
    ]
    assert enrich_model(model, warehouse, _options(C.ENUMS), SETTINGS, samples={}, synonyms={}).updates == ()


def test_sample_values_come_with_an_enum_decision_and_respect_what_is_written() -> None:
    model = _model(
        _column("status"),
        _column("city"),
        _column("amount", "column_type", column_type="fact"),
        _column("kind", "is_enum", is_enum=True),
        _column("level", "sample_values", "is_enum", sample_values=("hi", "lo"), is_enum=False),
        _column("tier", "sample_values", sample_values=("gold",)),
        _column("blank"),
        _column("email", sample_values=("a@b.c",), pii_tagged=True),
        _column("done", "sample_values", "is_enum", sample_values=("y", "n"), is_enum=True),
    )
    warehouse = [W(name.upper(), "TEXT") for name in ("status", "city", "kind", "level", "tier", "blank", "done")]
    warehouse.insert(2, W("AMOUNT", "NUMBER"))
    warehouse.append(W("EMAIL", "TEXT"))
    samples = {
        "status": ["open", "closed"],
        "city": ["a", "b", "c", "d"],
        "amount": ["5", "3", "1", "9"],
        "kind": ["k1", "k2", "k3", "k4"],
        "level": ["hi", "lo", "mid"],
        "tier": ["gold", "silver"],
        "blank": [],
        "email": ["x@y.z"],
        "done": ["n", "y"],
    }
    result = enrich_model(model, warehouse, _options(C.ENUMS), SETTINGS, samples=samples, synonyms={})
    assert [(update.name, update.values) for update in result.updates] == [
        ("status", (("sample_values", ("open", "closed")), ("is_enum", True))),
        ("city", (("sample_values", ("a", "b")), ("is_enum", False))),
        ("amount", (("sample_values", ("5", "3")),)),
        ("tier", (("is_enum", False),)),
    ]
    assert [(item.code, item.subject) for item in result.diagnostics] == [("SST-VAL328", "dbt_column:orders.email")]
    forced = enrich_model(
        model, warehouse, _options(C.ENUMS, forced=(C.ENUMS, C.SAMPLE_VALUES)), SETTINGS, samples=samples, synonyms={}
    )
    by_name = {update.name: update.values for update in forced.updates}
    assert by_name["kind"] == (("sample_values", ("k1", "k2")), ("is_enum", False))
    assert by_name["level"] == (("sample_values", ("hi", "lo", "mid")), ("is_enum", True))
    assert by_name["done"] == (("sample_values", ("n", "y")),)
    plain = enrich_model(model, warehouse, _options(C.SAMPLE_VALUES), SETTINGS, samples=samples, synonyms={})
    assert [update.name for update in plain.updates] == ["status", "city", "amount"]


def test_synonyms_fill_columns_without_any_and_described_columns_absent_from_the_relation_are_reported() -> None:
    model = _model(_column("a", synonyms=("x",)), _column("b"), _column("gone"), _column("c", synonyms=("same",)))
    warehouse = [W("A", "TEXT"), W("B", "TEXT"), W("C", "TEXT")]
    synonyms = {"a": ("alpha",), "b": ("bee",), "c": ("same",)}
    result = enrich_model(model, warehouse, _options(C.COLUMN_SYNONYMS), SETTINGS, samples={}, synonyms=synonyms)
    assert [(update.name, update.values) for update in result.updates] == [("b", (("synonyms", ("bee",)),))]
    assert [(item.code, item.subject) for item in result.diagnostics] == [("SST-VAL325", "dbt_column:orders.gone")]
    only_synonyms = frozenset({C.COLUMN_SYNONYMS})
    forced = enrich_model(
        model, warehouse, EnrichOptions(only_synonyms, only_synonyms), SETTINGS, samples={}, synonyms=synonyms
    )
    assert [update.name for update in forced.updates] == ["a", "b"]
    none = enrich_model(model, warehouse, _options(C.COLUMN_SYNONYMS), SETTINGS, samples={}, synonyms={})
    assert none.updates == ()


def test_prompts_name_every_column_and_the_names_to_avoid() -> None:
    columns = [PromptColumn("status", "TEXT", "Order\n state", ("open", "x" * 60)), PromptColumn("id", None, None)]
    prompt = column_synonyms_prompt("orders", None, columns, max_count=3)
    assert "Give at most 3 synonyms per column." in prompt
    assert "Description: none" in prompt
    assert "- status (TEXT): Order state Examples: open, " + "x" * 40 in prompt
    assert "- id (unknown type): none" in prompt
    table = table_synonyms_prompt("orders", "All orders", ["id"], ["customers"], max_count=2)
    assert "Columns: id" in table and "Avoid: customers" in table and "Give at most 2 synonyms." in table
    empty = table_synonyms_prompt("orders", None, [], [], max_count=2)
    assert "Columns: none" in empty and "Avoid: nothing" in empty
    assert COLUMN_SYNONYMS_SCHEMA["required"] == ["columns"]
    assert TABLE_SYNONYMS_SCHEMA["additionalProperties"] is False


def test_responses_are_read_only_in_the_shape_requested() -> None:
    response = {
        "columns": [
            {"name": " Status ", "synonyms": ["state"]},
            {"name": "id", "synonyms": "not a list"},
            "not an entry",
            {"name": 5, "synonyms": []},
        ]
    }
    assert parse_column_synonyms(response) == {"status": ("state",)}
    assert parse_column_synonyms({"columns": "nope"}) is None
    assert parse_column_synonyms(["not", "an", "object"]) is None
    assert parse_table_synonyms({"synonyms": ["sales"]}) == ("sales",)
    assert parse_table_synonyms({"synonyms": None}) is None
    assert parse_table_synonyms("text") is None


def test_table_synonyms_go_to_views_without_them_cleaned_per_view() -> None:
    bare = ViewTable("sales", "semantic_models/semantic_views/sales.yml", "orders", (), frozenset({"customers"}))
    named = ViewTable("menu", "semantic_models/semantic_views/menu.yml", "orders", ("purchases",), frozenset())
    targets = [bare, named]
    assert not needs_table_synonyms(targets, _options())
    assert needs_table_synonyms(targets, _options(C.TABLE_SYNONYMS))
    assert not needs_table_synonyms([named], _options(C.TABLE_SYNONYMS))
    assert needs_table_synonyms([named], _options(forced=(C.TABLE_SYNONYMS,)))
    assert avoided_names(targets) == ("customers",)
    proposals = ["customers", "sales orders", "purchases"]
    assert table_synonym_edits(targets, proposals, _options(), limit=4) == ()
    edits = table_synonym_edits(targets, proposals, _options(C.TABLE_SYNONYMS), limit=4)
    assert [(edit.view, edit.synonyms) for edit in edits] == [("sales", ("sales orders", "purchases"))]
    forced = table_synonym_edits(targets, ["purchases"], _options(forced=(C.TABLE_SYNONYMS,)), limit=4)
    assert [edit.view for edit in forced] == ["sales"]
    assert table_synonym_edits([bare], ["customers"], _options(C.TABLE_SYNONYMS), limit=4) == ()
