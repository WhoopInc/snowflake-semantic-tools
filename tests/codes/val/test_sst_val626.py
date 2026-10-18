"""SST-VAL626: a routine's body hard-codes the relation a project model or source resolves to."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel

_DBT = DbtCatalog("v12", "1.11", "p", (DbtModel("model.p.orders", "orders", "SALES_DB.MARTS.ORDERS", (), (), ()),))
_TOOL = """
tool:
  name: order_status
  type: procedure
  language: sql
  returns: VARCHAR
  body: "{{ file('order_status.sql') }}"
"""


def _reported(tmp_path: Path, body: str) -> list[Diagnostic]:
    folder = tmp_path / "tools"
    folder.mkdir()
    (folder / "order_status.yml").write_text(_TOOL, encoding="utf-8")
    (folder / "order_status.sql").write_text(body, encoding="utf-8")
    catalog = load_tool_catalog(tmp_path, _DBT, target_name="dev", declared_targets=frozenset(("dev",)))
    return [item for item in catalog.diagnostics if item.code == "SST-VAL626"]


def test_sst_val626_fires(tmp_path: Path) -> None:
    [diagnostic] = _reported(tmp_path, "BEGIN\n  RETURN (SELECT MAX(status) FROM sales_db.marts.orders);\nEND\n")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "tool member 'order_status': body hard-codes SALES_DB.MARTS.ORDERS, which is {{ ref('orders') }}"
    )
    assert diagnostic.origin == Origin("tools/order_status.sql", 2, 35)
    assert diagnostic.subject == "tool:order_status"


def test_sst_val626_silent(tmp_path: Path) -> None:
    body = (
        "BEGIN\n"
        "  -- once read SALES_DB.MARTS.ORDERS\n"
        "  RETURN (SELECT MAX(status) FROM {{ ref('orders') }} JOIN PARTNER.DELIVERY.FEES USING (id));\n"
        "END\n"
    )
    assert _reported(tmp_path, body) == []
