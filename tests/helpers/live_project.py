"""The small project the live layers publish: the reference `orders` model, one view, two metrics.

`live_project` writes it under a root for one scratch schema: a dbt manifest holding only the
reference project's `orders` model, re-pointed at an `ORDERS` table in that schema; a profile
whose `live` target reads every credential from the environment; and an `sst_config.yml` that
publishes the view, and keeps state, in the same schema. `orders_table_statements` creates and
fills the table the view reads, typed from the manifest's own columns.

Offline, the same project compiles and validates with `--no-snowflake-syntax-check`, which is how
`tests/unit/test_live_project.py` proves its shape without a connection.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import Sql, datatype, ident, join, literal, qname, sql
from tests.helpers.live_snowflake import profile_target
from tests.helpers.reference_project import DBT_MANIFEST

PROFILE = "sst_live"
TARGET = "live"
VIEW = "live_orders"
_ORDERS = "model.sst_reference_impl.orders"
_ROWS = (
    ("o-1", "c-1", "l-1", "2026-01-01 10:00:00", "completed", "10.00", "0.80", "10.80"),
    ("o-2", "c-1", "l-1", "2026-01-02 11:00:00", "returned", "20.00", "1.60", "21.60"),
)

_CONFIG = """\
validation:
  snowflake_syntax_check: true
semantic_views:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"
state:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"
  +table: SST_STATE
"""
_VIEWS = f"""\
semantic_views:
  - name: {VIEW}
    description: |-
      Use this view for questions about how many orders were placed and what they were worth.
    tables:
      - "{{{{ ref('orders') }}}}"
"""
_METRICS = """\
snowflake_metrics:
  - name: order_count
    tables:
      - orders
    description: |-
      Number of distinct orders placed.
    expr: "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"
  - name: total_revenue
    tables:
      - orders
    description: |-
      Total order value including tax.
    expr: "SUM({{ ref('orders', 'order_total') }})"
"""


def live_project(root: Path, schema: SchemaScope) -> tuple[Path, Path]:
    """Write the live project under `root` for `schema`; return the project and its dbt manifest."""
    project = root / "live_project"
    files = {
        "dbt_project.yml": yaml.safe_dump(
            {"name": "sst_reference_impl", "version": "1.0.0", "config-version": 2, "profile": PROFILE}
        ),
        "profiles.yml": yaml.safe_dump(
            {PROFILE: {"target": TARGET, "outputs": {TARGET: profile_target(schema.schema.value)}}}, sort_keys=False
        ),
        "sst_config.yml": _CONFIG,
        "semantic_models/semantic_views/live.yml": _VIEWS,
        "semantic_models/metrics/live.yml": _METRICS,
    }
    for relative, text in files.items():
        path = project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    manifest = root / "live_manifest.json"
    manifest.write_text(json.dumps(_manifest(schema)), encoding="utf-8")
    return project, manifest


def orders_table(schema: SchemaScope) -> QualifiedName:
    """The table the live view reads."""
    return QualifiedName(schema.database, schema.schema, Identifier.parse("ORDERS"))


def live_view(schema: SchemaScope) -> QualifiedName:
    """The semantic view `apply` publishes for the live project."""
    return QualifiedName(schema.database, schema.schema, Identifier.parse(VIEW))


def project_args(project: Path, manifest: Path) -> list[str]:
    """The options every `sst` command over the live project takes."""
    return ["--project-dir", str(project), "--profiles-dir", str(project), "--manifest", str(manifest)]


def published_project(root: Path, port: ExecutionPort, schema: SchemaScope) -> tuple[Path, Path]:
    """Create and fill `ORDERS` in `schema` through `port`, then write the live project over it.

    Raises:
        RuntimeError: Snowflake refused to create or fill the table.
    """
    made = port.execute_script(orders_table_statements(schema))
    if not made.ok:
        raise RuntimeError(f"could not create {orders_table(schema).sql}: {made.error}")
    return live_project(root, schema)


def orders_table_statements(schema: SchemaScope) -> tuple[Sql, ...]:
    """Create the `ORDERS` table in `schema` with the manifest's columns, and insert two orders."""
    columns = _orders_node()["columns"]
    definition = join(
        ", ",
        (
            sql("{name} {type}", name=ident(Identifier.parse(name)), type=datatype(column["data_type"]))
            for name, column in columns.items()
        ),
    )
    rows = join(", ", (sql("({values})", values=join(", ", (literal(value) for value in row))) for row in _ROWS))
    table = qname(orders_table(schema))
    return (
        sql("CREATE TABLE {table} ({columns})", table=table, columns=definition),
        sql("INSERT INTO {table} VALUES {rows}", table=table, rows=rows),
    )


def _orders_node() -> dict[str, Any]:
    document: dict[str, Any] = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    node: dict[str, Any] = document["nodes"][_ORDERS]
    return node


def _manifest(schema: SchemaScope) -> dict[str, Any]:
    """The reference manifest with only `orders` and its tests, re-pointed at the scratch schema."""
    document: dict[str, Any] = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    node = document["nodes"][_ORDERS]
    node.update(
        database=schema.database.value,
        schema=schema.schema.value,
        alias="ORDERS",
        relation_name=orders_table(schema).sql,
    )
    tests = {
        key: test
        for key, test in document["nodes"].items()
        if test["resource_type"] == "test" and test.get("attached_node") == _ORDERS
    }
    document["nodes"] = {_ORDERS: node, **tests}
    return document
