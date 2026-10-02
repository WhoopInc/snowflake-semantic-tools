"""SST-PRS125: writing a model file `sst enrich` edits would also change lines it did not edit."""

from __future__ import annotations

from snowflake_semantic_tools.app.enrich import EnrichProject, EnrichRequest
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from tests.helpers.enrich_ports import InMemoryFiles, ScriptedEnrich
from tests.helpers.project_inputs import InMemoryProjectInputs

MODEL = DbtModel(
    "model.shop.orders",
    "orders",
    "DB.SCH.ORDERS",
    ("order_id",),
    (),
    (DbtColumn("order_id", None, None, None),),
    package_name="shop",
    raw_relation_name="db.sch.orders",
    patch_file="models/orders.yml",
    original_file_path="models/orders.sql",
)


def _run(text: str) -> EnrichProject:
    inputs = InMemoryProjectInputs(dbt=DbtCatalog("v12", "1.11.2", "shop", (MODEL,)), views=SemanticViewProject(()))
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": [("ORDER_ID", "TEXT")]})
    return EnrichProject(inputs, port, InMemoryFiles({"models/orders.yml": text}))


def test_sst_prs125_fires() -> None:
    # A list indented flush with its key is rewritten indented, a line enrich did not edit.
    report = _run("models:\n  - name: orders\n    data_tests:\n    - x\n").run(EnrichRequest())
    [diagnostic] = [item for item in report.diagnostics if item.code == "SST-PRS125"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "models/orders.yml: writing it changes lines sst enrich did not edit"
    assert diagnostic.origin == Origin("models/orders.yml")


def test_sst_prs125_silent() -> None:
    report = _run("version: 2\nmodels:\n  - name: orders\n    columns:\n      - name: order_id\n").run(EnrichRequest())
    assert "SST-PRS125" not in [item.code for item in report.diagnostics]
