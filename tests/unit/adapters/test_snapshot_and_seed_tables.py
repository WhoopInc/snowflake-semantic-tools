"""A dbt snapshot or seed is a semantic-view table as a model is: `ref()` names it, and it renders."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.projects import load_project
from tests.helpers.reference_project import project_copy

# A manifest the reference project's dbt wrote, with its snapshot and seeds in it.
MANIFEST = (
    Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "dbt_manifests" / "v12" / "dbt-2.0.8" / "manifest.json"
)

HISTORY = """\
semantic_view:
  name: customer_history
  description: |-
    Use this view for questions about orders by the customer's details as they were when each
    order was placed, rather than as they are now.
  tables:
    - "{{ ref('orders') }}"
    - "{{ ref('customers_snapshot') }}"
  # The variables the metrics `orders` brings with it read.
  variables:
    - name: large_order_cents
      data_type: NUMBER
      description: Threshold above which an order counts as large.
      default_value: 1000
    - name: tax_inclusive
      data_type: BOOLEAN
      description: Whether revenue metrics should include collected tax.
      default_value: false
"""
HISTORY_RELATIONSHIP = """\
relationship:
  name: orders_to_customer_history
  description: Each order matches the version of its customer that was current when it was placed.
  left_table: "{{ ref('orders') }}"
  right_table: "{{ ref('customers_snapshot') }}"
  relationship_conditions:
    - "{{ ref('orders', 'customer_id') }} = {{ ref('customers_snapshot', 'customer_id') }}"
    - "{{ ref('orders', 'ordered_at') }} >= {{ ref('customers_snapshot', 'dbt_valid_from') }}"
"""
RAW = """\
semantic_view:
  name: raw_products
  description: Use this view for questions about the product catalog as its seed loads it.
  tables:
    - "{{ ref('products_raw') }}"
"""


def test_a_view_over_a_snapshot_joins_it_asof_and_a_view_over_a_seed_renders(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    views_dir = project / "semantic_models" / "semantic_views"
    (views_dir / "customer_history.yml").write_text(HISTORY, encoding="utf-8")
    (views_dir / "raw_products.yml").write_text(RAW, encoding="utf-8")
    (project / "semantic_models" / "relationships" / "orders_to_customer_history.yml").write_text(
        HISTORY_RELATIONSHIP, encoding="utf-8"
    )
    loaded = load_project(project, manifest_path=MANIFEST)
    views = {view.fqn.rsplit(".", 1)[1]: view for view in loaded.views}
    # The seed's ref() resolves, so nothing says it names no model; its view fails only for
    # what any table with undocumented columns would (SST-VAL323, SST-VAL301).
    codes = {item.code for item in loaded.diagnostics if item.severity.name == "ERROR"}
    assert "SST-REF001" not in codes
    assert codes <= {"SST-VAL323", "SST-VAL301"}, codes

    history = views["CUSTOMER_HISTORY"]
    snapshot = next(table for table in history.tables if table.logical_name == "CUSTOMERS_SNAPSHOT")
    assert snapshot.fqn == "SST_REF_DEV.JAFFLE.CUSTOMERS_SNAPSHOT"
    [relationship] = [item for item in history.relationships if item.name == "ORDERS_TO_CUSTOMER_HISTORY"]
    assert relationship.to_columns == ("CUSTOMER_ID", "DBT_VALID_FROM")
    assert relationship.asof_index == 1
    ddl = str(render(history))
    assert "CUSTOMERS_SNAPSHOT AS SST_REF_DEV.JAFFLE.CUSTOMERS_SNAPSHOT" in ddl
    assert "ASOF DBT_VALID_FROM" in ddl
    assert {item.subject for item in loaded.diagnostics if item.code == "SST-VAL323"} == {"semantic_view:raw_products"}
