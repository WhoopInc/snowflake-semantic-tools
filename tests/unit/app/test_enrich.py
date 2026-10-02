"""`sst enrich` as a use case: what it selects, reads, decides, and writes."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from snowflake_semantic_tools.app.enrich import (
    EditedFile,
    EnrichProject,
    EnrichReport,
    EnrichRequest,
    model_file,
    relation,
    select_models,
    view_tables,
)
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.enrich import COLUMNS_PER_PROMPT, Component, resolve_options
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView, Table
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
from tests.helpers.enrich_ports import InMemoryFiles, ScriptedEnrich
from tests.helpers.project_inputs import InMemoryProjectInputs

C = Component
ORDERS_YML = "version: 2\nmodels:\n  - name: orders\n    columns:\n      - name: order_id\n      - name: total\n"
ORDERS_COLUMNS = [("ORDER_ID", "TEXT"), ("TOTAL", "NUMBER"), ("PLACED_AT", "TIMESTAMP_NTZ")]


def _model(name: str = "orders", columns: tuple[DbtColumn, ...] | None = None, **fields: object) -> DbtModel:
    values: dict[str, object] = {
        "package_name": "shop",
        "raw_relation_name": f"db.sch.{name}",
        "patch_file": f"models/{name}.yml",
        "original_file_path": f"models/{name}.sql",
        "description": f"The {name}.",
    }
    values.update(fields)
    described = columns or (DbtColumn("order_id", None, None, None), DbtColumn("total", None, None, None))
    return DbtModel(f"model.shop.{name}", name, f"DB.SCH.{name.upper()}", ("order_id",), (), described, **values)  # type: ignore[arg-type]


def _catalog(*models: DbtModel, relationless: tuple[str, ...] = ()) -> DbtCatalog:
    return DbtCatalog("v12", "1.11.2", "shop", models, relationless_models=relationless)


def _project(
    *models: DbtModel,
    port: ScriptedEnrich | None = None,
    files: InMemoryFiles | None = None,
    tree: Mapping[str, object] | None = None,
    views: tuple[SemanticView, ...] = (),
) -> tuple[EnrichProject, ScriptedEnrich, InMemoryFiles]:
    inputs = InMemoryProjectInputs(
        tree=tree or {}, dbt=_catalog(*(models or (_model(),))), views=SemanticViewProject(views)
    )
    port = port or ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    files = files or InMemoryFiles({"models/orders.yml": ORDERS_YML})
    return EnrichProject(inputs, port, files), port, files


def _request(*included: Component, forced: tuple[Component, ...] = (), **fields: object) -> EnrichRequest:
    return EnrichRequest(options=resolve_options(frozenset(included), frozenset(forced)), **fields)  # type: ignore[arg-type]


def test_the_default_run_fills_types_and_adds_the_relations_new_columns() -> None:
    project, port, files = _project()
    report = project.run(_request())
    assert port.calls == [("columns", "DB.SCH.ORDERS")]
    assert [(item.path, item.changed, item.reformatted) for item in report.files] == [
        ("models/orders.yml", True, False)
    ]
    (orders,) = report.models
    assert orders.enrichment is not None and orders.enrichment.added == ("placed_at",)
    assert (orders.enrichment.filled(C.COLUMN_TYPES), orders.enrichment.filled(C.DATA_TYPES)) == (3, 3)
    assert list(report.diagnostics) == [] and report.selected == 1 and not report.stopped
    assert project.write(report) == ("models/orders.yml",)
    written = files.texts["models/orders.yml"]
    assert "      - name: placed_at\n        config:\n          meta:\n            sst:\n" in written
    assert "              column_type: fact\n              data_type: NUMBER\n" in written
    assert files.written == ["models/orders.yml"]


def test_a_second_run_over_its_own_output_changes_nothing() -> None:
    project, _, files = _project()
    project.write(project.run(_request()))
    columns = (
        DbtColumn("order_id", None, "TEXT", "dimension", declared_keys=frozenset({"column_type", "data_type"})),
        DbtColumn("total", None, "NUMBER", "fact", declared_keys=frozenset({"column_type", "data_type"})),
        DbtColumn(
            "placed_at", None, "TIMESTAMP_NTZ", "dimension", declared_keys=frozenset({"column_type", "data_type"})
        ),
    )
    again, _, _ = _project(_model(columns=columns), files=files)
    report = again.run(_request())
    assert report.files == () and report.changed == () and again.write(report) == ()


def test_selection_keeps_the_projects_models_under_the_paths_and_names_given() -> None:
    orders, refunds = _model(), _model("refunds", original_file_path="models/finance/refunds.sql", patch_file=None)
    vendored = _model("vendored", package_name="dbt_utils")
    catalog = _catalog(refunds, orders, vendored, relationless=("helper",))

    def names(request: EnrichRequest) -> list[str]:
        return [model.name for model in select_models(catalog, request)[0]]

    assert names(EnrichRequest()) == ["orders", "refunds"]
    assert names(EnrichRequest(paths=("models/finance/",))) == ["refunds"]
    assert names(EnrichRequest(paths=("models/orders.yml",))) == ["orders"]
    assert names(EnrichRequest(selected=("REF*",))) == ["refunds"]
    assert names(EnrichRequest(excluded=("orders",))) == ["refunds"]
    _, found = select_models(catalog, EnrichRequest(selected=("helper", "orders")))
    assert [(item.code, item.subject) for item in found] == [("SST-DBT031", "dbt_model:helper")]
    assert select_models(catalog, EnrichRequest(selected=("helper",), excluded=("helper",)))[1] == []


def test_a_projects_refusal_to_collect_row_data_stops_the_run_before_any_read() -> None:
    tree = {"enrichment": {"allow_sample_value_collection": False}}
    project, port, _ = _project(tree=tree)
    report = project.run(_request(C.SAMPLE_VALUES))
    assert [item.code for item in report.diagnostics] == ["SST-CFG038"]
    assert report.stopped and port.calls == [] and project.write(report) == ()


def test_a_missing_relation_fails_its_model_and_the_others_are_still_enriched() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    files = InMemoryFiles(
        {"models/orders.yml": ORDERS_YML, "models/refunds.yml": ORDERS_YML.replace("orders", "refunds")}
    )
    project, _, _ = _project(_model(), _model("refunds"), port=port, files=files)
    report = project.run(_request())
    assert [(item.model, item.failed) for item in report.models] == [("orders", False), ("refunds", True)]
    assert [(item.code, item.severity) for item in report.diagnostics] == [("SST-SNO030", Severity.ERROR)]
    assert "DB.SCH.REFUNDS does not exist" in report.diagnostics[0].message
    assert [item.path for item in report.writable] == ["models/orders.yml"]


def test_fail_fast_stops_at_the_first_failure_and_writes_nothing() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.REFUNDS": ORDERS_COLUMNS})
    files = InMemoryFiles(
        {"models/orders.yml": ORDERS_YML, "models/refunds.yml": ORDERS_YML.replace("orders", "refunds")}
    )
    project, _, _ = _project(_model(), _model("refunds"), port=port, files=files)
    report = project.run(_request(fail_fast=True))
    assert [item.model for item in report.models] == ["orders"] and report.stopped
    assert report.files == () and report.writable == ()


def test_a_query_failure_fails_one_model_and_a_connection_failure_stops_the_run() -> None:
    broken = ScriptedEnrich(failures={"DB.SCH.ORDERS": SnowflakePortError("Object does not exist\nmore detail")})
    project, _, _ = _project(port=broken)
    report = project.run(_request())
    assert [item.code for item in report.diagnostics] == ["SST-SNO031"]
    assert report.diagnostics[0].message == "model 'orders': reading columns failed: Object does not exist"
    refused = SnowflakePortError("denied", diagnostic=D("SST-PRT004", value="the statement", detail="denied"))
    project, _, _ = _project(port=ScriptedEnrich(failures={"DB.SCH.ORDERS": refused}))
    with pytest.raises(SnowflakePortError, match="denied"):
        project.run(_request())
    silent = ScriptedEnrich(failures={"DB.SCH.ORDERS": SnowflakePortError("  ")})
    project, _, _ = _project(port=silent)
    assert project.run(_request()).diagnostics[0].message.endswith("failed: SnowflakePortError")


def test_samples_are_read_once_with_one_more_value_than_the_distinct_limit() -> None:
    port = ScriptedEnrich(
        columns={"DB.SCH.ORDERS": ORDERS_COLUMNS},
        values={"DB.SCH.ORDERS": {"ORDER_ID": ["a", "b"], "TOTAL": ["3", "1"], "PLACED_AT": []}},
    )
    project, _, files = _project(
        port=port, tree={"enrichment": {"distinct_limit": 2, "sample_values_display_limit": 1}}
    )
    report = project.run(_request(C.SAMPLE_VALUES))
    assert port.calls[1] == ("values", "DB.SCH.ORDERS", ("ORDER_ID", "TOTAL", "PLACED_AT"), 3)
    project.write(report)
    assert (
        "sample_values:\n                - a\n                - b\n              is_enum: true\n"
        in files.texts["models/orders.yml"]
    )
    sampling_fails = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    sampling_fails.distinct_values = lambda *args: (_ for _ in ()).throw(SnowflakePortError("too slow"))  # type: ignore[method-assign]
    project, _, _ = _project(port=sampling_fails)
    assert project.run(_request(C.SAMPLE_VALUES)).diagnostics[0].message.endswith("sampling values failed: too slow")


def test_nothing_is_sampled_when_no_column_needs_values() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": [("PAYLOAD", "VARIANT")]})
    project, _, _ = _project(port=port)
    project.run(_request(C.SAMPLE_VALUES))
    assert [call[0] for call in port.calls] == ["columns"]


def test_column_synonyms_are_asked_in_batches_and_a_malformed_answer_fails_the_model() -> None:
    wide = [(f"C{index}", "TEXT") for index in range(COLUMNS_PER_PROMPT + 1)]

    def answer(prompt: str, schema: object) -> object:
        names = [line[2:].split(" ")[0] for line in prompt.splitlines() if line.startswith("- ")]
        return {"columns": [{"name": name, "synonyms": [f"{name} alias"]} for name in names]}

    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": wide}, answer=answer)
    project, _, _ = _project(port=port)
    report = project.run(_request(C.COLUMN_SYNONYMS))
    assert [call for call in port.calls if call[0] == "cortex"] == [("cortex", "mistral-large2")] * 2
    enrichment = report.models[0].enrichment
    assert enrichment is not None and enrichment.filled(C.COLUMN_SYNONYMS) == COLUMNS_PER_PROMPT + 1
    malformed = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, answer=lambda *_: ["not", "an", "object"])
    project, _, _ = _project(port=malformed)
    failed = project.run(_request(C.COLUMN_SYNONYMS))
    assert failed.models[0].failed
    assert failed.diagnostics[0].message.endswith(
        "column synonyms failed: Cortex answered in another shape than the one requested"
    )
    unavailable = ScriptedEnrich(
        columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, cortex_failure=SnowflakePortError("no model")
    )
    project, _, _ = _project(port=unavailable)
    assert project.run(_request(C.COLUMN_SYNONYMS)).diagnostics[0].message.endswith("column synonyms failed: no model")


VIEW_YML = (
    "semantic_views:\n  - name: sales\n    tables:\n"
    "      - \"{{ ref('orders') }}\"\n      - \"{{ ref('customers') }}\"\n"
)


def _view(source_path: str | None = "semantic_models/views.yml", synonyms: tuple[str, ...] = ()) -> SemanticView:
    tables = (
        Table("ORDERS", "DB.SCH.ORDERS", synonyms=synonyms),
        Table("CUSTOMERS", "DB.SCH.CUSTOMERS", synonyms=("buyer",)),
    )
    return SemanticView("DB.SCH.SALES", tables, source_path=source_path, referenced_models=("customers", "orders"))


def test_table_synonyms_are_generated_once_and_written_into_each_views_table_config() -> None:
    port = ScriptedEnrich(
        columns={"DB.SCH.ORDERS": ORDERS_COLUMNS},
        answer=lambda prompt, schema: {"synonyms": ["buyer", "sales orders", "purchases"]},
    )
    files = InMemoryFiles({"models/orders.yml": ORDERS_YML, "semantic_models/views.yml": VIEW_YML})
    project, _, _ = _project(port=port, files=files, views=(_view(), _view(source_path=None)))
    report = project.run(_request(C.TABLE_SYNONYMS))
    assert "Avoid: buyer, customers" in port.prompts[0]
    assert [(edit.view, edit.synonyms) for edit in report.models[0].table_synonyms] == [
        ("SALES", ("sales orders", "purchases"))
    ]
    project.write(report)
    assert files.texts["semantic_models/views.yml"].endswith(
        "    table_config:\n      orders:\n        synonyms:\n          - sales orders\n          - purchases\n"
    )
    project, port, _ = _project(
        port=ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}), views=(_view(synonyms=("x",)),)
    )
    project.run(_request(C.TABLE_SYNONYMS))
    assert [call[0] for call in port.calls] == ["columns"]
    malformed = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, answer=lambda *_: {"other": 1})
    project, _, _ = _project(port=malformed, views=(_view(),))
    assert (
        project.run(_request(C.TABLE_SYNONYMS))
        .diagnostics[0]
        .message.endswith("table synonyms failed: Cortex answered in another shape than the one requested")
    )


def test_a_file_written_with_other_formatting_is_reported() -> None:
    files = InMemoryFiles({"models/orders.yml": "models:\n  - name: orders\n    data_tests:\n    - x\n"})
    project, _, _ = _project(files=files)
    report = project.run(_request())
    assert [(item.code, item.origin.file if item.origin else None) for item in report.diagnostics] == [
        ("SST-PRS125", "models/orders.yml")
    ]


def test_new_yaml_files_sit_beside_the_models_sql_and_relations_take_the_overrides() -> None:
    assert model_file(_model(patch_file=None)) == "models/orders.yml"
    assert model_file(_model(patch_file=None, original_file_path="models/x/orders")) == "models/x/orders.yml"
    assert model_file(_model(patch_file=None, original_file_path=None)) == "models/orders.yml"
    request = EnrichRequest(database=Identifier.parse("dev_db"), schema=Identifier.parse('"Mixed"'))
    assert relation(_model(), request).sql == 'DEV_DB."Mixed".ORDERS'
    assert relation(_model(raw_relation_name=None), EnrichRequest()).sql == "DB.SCH.ORDERS"
    project, _, files = _project(_model(patch_file=None, original_file_path="models/new/orders.sql"))
    report = project.run(_request())
    assert [(item.path, item.before) for item in report.files] == [
        ("models/new/orders.sql".replace(".sql", ".yml"), None)
    ]


def test_view_tables_describe_only_views_that_use_the_model() -> None:
    other = SemanticView(
        "DB.SCH.OTHER", (Table("CUSTOMERS", "DB.SCH.CUSTOMERS"),), source_path="v.yml", referenced_models=("customers",)
    )
    (target,) = view_tables([_view(synonyms=("sale",)), other], "orders")
    assert (target.view, target.path, target.table, target.synonyms) == (
        "SALES",
        "semantic_models/views.yml",
        "orders",
        ("sale",),
    )
    assert target.taken == {"customers", "buyer"}


def test_an_empty_report_has_nothing_to_write() -> None:
    report = EnrichReport(files=(EditedFile("a.yml", "x", "x"),))
    assert report.changed == () and report.writable == ()


def test_the_enrichment_blocks_own_config_problems_are_reported_and_its_errors_stop_the_run() -> None:
    bad_limit = D(
        "SST-CFG008",
        subject="config:enrichment.distinct_limit",
        key="enrichment.distinct_limit",
        found="0",
        expected="1..1000",
    )
    unknown = D("SST-CFG003", subject="config:enrichment.infer", key="enrichment.infer")
    elsewhere = D("SST-CFG003", subject="config:generation", key="generation")
    inputs = InMemoryProjectInputs(
        dbt=_catalog(_model()), config_diagnostics=DiagnosticBag((bad_limit, unknown, elsewhere))
    )
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    stopped = EnrichProject(inputs, port, InMemoryFiles({"models/orders.yml": ORDERS_YML})).run(_request())
    assert [item.code for item in stopped.diagnostics] == ["SST-CFG008", "SST-CFG003"]
    assert stopped.stopped and port.calls == []
    warned = InMemoryProjectInputs(dbt=_catalog(_model()), config_diagnostics=DiagnosticBag((unknown, elsewhere)))
    report = EnrichProject(warned, port, InMemoryFiles({"models/orders.yml": ORDERS_YML})).run(_request())
    assert [item.code for item in report.diagnostics] == ["SST-CFG003"] and not report.stopped
