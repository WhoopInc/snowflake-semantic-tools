"""`sst enrich --include all` over the reference project, compared byte for byte with its goldens.

The warehouse and Cortex are scripted here, by hand: every value below is invented for the
fixture, and nothing is collected from a warehouse. Each scripted answer is chosen to show one
rule at work, named beside it. When a change to enrich's output is intended, run this test to
see the diff, edit the golden by hand, and say why in the pull request.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.reference_project import DBT_MANIFEST, REPO_ROOT, project_copy

GOLDEN = REPO_ROOT / "tests" / "golden" / "expected" / "enrich"
CHANGED = (
    "models/marts/locations.yml",
    "models/marts/order_items.yml",
    "semantic_models/semantic_views/core/semantic_views.yml",
    "semantic_models/semantic_views/semantic_views.yml",
)

COLUMNS = {
    "SST_REF_DEV.JAFFLE.ORDER_ITEMS": [
        ("ORDER_ITEM_ID", "TEXT"),
        ("ORDER_ID", "TEXT"),
        ("PRODUCT_ID", "TEXT"),
        ("OCCURRED_AT", "TIMESTAMP_NTZ"),
        ("ITEM_PRICE", "NUMBER"),
        # Columns the YAML does not describe yet: added in the relation's order.
        ("QUANTITY", "NUMBER"),
        ("LINE_STATUS", "TEXT"),
    ],
    "SST_REF_DEV.JAFFLE.LOCATIONS": [
        ("LOCATION_ID", "TEXT"),
        ("LOCATION_NAME", "TEXT"),
        ("OPENED_AT", "TIMESTAMP_NTZ"),
        ("TAX_RATE", "NUMBER"),
        ("REGION", "TEXT"),
    ],
}

VALUES = {
    "SST_REF_DEV.JAFFLE.ORDER_ITEMS": {
        # More distinct values than the limit: the ten most frequent, and is_enum false.
        "ORDER_ITEM_ID": [f"OI-{index:03d}" for index in range(1, 27)],
        "ORDER_ID": [f"O-{index:03d}" for index in range(1, 27)],
        # Every value read: an enum.
        "PRODUCT_ID": ["JAF-001", "JAF-002", "BEV-001", "BEV-002"],
        "OCCURRED_AT": [f"2024-07-{day:02d} 12:00:00.000" for day in range(1, 27)],
        # A fact: sample values, never is_enum.
        "ITEM_PRICE": ["7.50", "3.00", "12.00", "5.00"],
        "QUANTITY": ["1", "2", "3"],
        # 'nan' is a placeholder, never written, so the set is not complete: no enum.
        "LINE_STATUS": ["fulfilled", "refunded", "nan"],
    },
    "SST_REF_DEV.JAFFLE.LOCATIONS": {
        # Its written is_enum: false is kept; only the missing sample values are filled.
        "LOCATION_ID": ["L-01", "L-02", "L-03"],
        # Its written sample values are kept; they are not every value, so is_enum is false.
        "OPENED_AT": ["2023-03-01 08:00:00.000", "2023-09-15 08:00:00.000", "2024-01-10 08:00:00.000"],
        "REGION": ["east", "west"],
    },
}

COLUMN_SYNONYMS = {
    # 'Order Item ID' is the column's own name.
    "order_item_id": ["line item id", "Order Item ID", "order line"],
    # 'line item id' is already order_item_id's.
    "order_id": ["order number", "line item id"],
    "product_id": ["sku", "product code"],
    "occurred_at": ["sold at", "sale time"],
    # A quote cannot be written into a synonym.
    "item_price": ["unit price", "price" + chr(39) + "s"],
    "quantity": ["units", "qty"],
    # Nor can template syntax.
    "line_status": ["status", "{{ status }}"],
    "region": ["area", "territory"],
}

TABLE_SYNONYMS = {
    "order_items": ["line items", "order lines"],
    # 'buyer' is a synonym of customers in jaffle_sales, the view locations is in.
    "locations": ["store", "shop", "buyer", "branch"],
}


def _answer(prompt: str, schema: object) -> object:
    lines = prompt.splitlines()
    if "for the columns of one table" in prompt:
        names = [line[2:].split(" ")[0] for line in lines if line.startswith("- ")]
        return {"columns": [{"name": name, "synonyms": COLUMN_SYNONYMS[name]} for name in names]}
    table = next(line.split(": ", 1)[1] for line in lines if line.startswith("Table: "))
    return {"synonyms": TABLE_SYNONYMS[table]}


class _Warehouse(ScriptedEnrich):
    def current_role(self) -> str:
        return "ANALYST"

    def current_account_locator(self) -> str:
        return "ACCOUNT"

    def close(self) -> None:
        return None


def test_enrich_writes_the_golden_yaml(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    before = {path: (project / path).read_text(encoding="utf-8") for path in CHANGED}
    port = _Warehouse(columns=COLUMNS, values=VALUES, answer=_answer)
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    args = ["enrich", "--select", "order_items", "--select", "locations", "--include", "all", "--allow-non-prod"]
    result = CliRunner().invoke(cli, [*args, "--project-dir", str(project), "--manifest", str(DBT_MANIFEST)])
    assert result.exit_code == 0, result.output
    for path in CHANGED:
        written = (project / path).read_text(encoding="utf-8")
        assert written != before[path], path
        assert written == (GOLDEN / path).read_text(encoding="utf-8"), path
