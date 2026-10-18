"""A procedure whose body reads a model through `ref()`, published to two targets against a real account.

Each target has its own `ORDERS` table and its own dbt manifest, as dbt builds one per target,
so the same body file publishes a procedure on each target that counts that target's orders.
Lineage then runs from the model to the procedure, and an edit planned with `--no-diff` still
names the member it changes, from the fingerprints the state table recorded at apply.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
import yaml

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.sql import qname, sql
from tests.helpers.e2e_cli import run_sst
from tests.helpers.file_trees import write_tree
from tests.helpers.live_project import (
    PROFILE,
    live_project,
    orders_table,
    orders_table_statements,
    project_args,
    published_project,
)
from tests.helpers.live_snowflake import LiveAccount, profile_target

pytestmark = [pytest.mark.live, pytest.mark.e2e]

_TOOLS = """\
tools:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"
  +warehouse: "{{ target.warehouse }}"
"""
_PROCEDURE = """\
tool:
  name: order_count
  type: procedure
  description: Counts the orders this target holds.
  language: sql
  returns: NUMBER
  body: "{{ file('order_count.sql') }}"
"""
_BODY = """\
BEGIN
  RETURN (SELECT COUNT(*) FROM {{ ref('orders') }});
END
"""


def test_a_routine_reads_each_target_s_own_relation_and_lineage_and_state_follow_it(
    live_account: LiveAccount,
    live_connector: SnowflakeConnector,
    scratch_schema: Callable[[str], SchemaScope],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    first, second = scratch_schema("refa"), scratch_schema("refb")
    root = tmp_path_factory.mktemp("routine")
    project, first_manifest = published_project(root, live_connector, first, key_pair=live_account.key_pair)
    made = live_connector.execute_script(orders_table_statements(second))
    assert made.ok, made.error
    extra = live_connector.execute_script(
        (sql("INSERT INTO {table} SELECT * FROM {table} LIMIT 1", table=qname(orders_table(second))),)
    )
    assert extra.ok, extra.error
    _, second_manifest = live_project(tmp_path_factory.mktemp("routine_b"), second, key_pair=live_account.key_pair)
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8") + _TOOLS, encoding="utf-8")
    write_tree(project, {"tools/orders/order_count.yml": _PROCEDURE, "tools/orders/order_count.sql": _BODY})
    outputs = {
        name: profile_target(scope.schema.value, key_pair=live_account.key_pair)
        for name, scope in (("a", first), ("b", second))
    }
    (project / "profiles.yml").write_text(
        yaml.safe_dump({PROFILE: {"target": "a", "outputs": outputs}}, sort_keys=False), encoding="utf-8"
    )
    manifests = {"a": first_manifest, "b": second_manifest}
    for target, scope in (("a", first), ("b", second)):
        args = project_args(project, manifests[target])
        compiled = run_sst("--output", "json", "compile", *args, "--target", target)
        assert compiled.exit_code == 0, compiled.stdout
        applied = run_sst("--output", "json", "apply", *args, "--target", target, "--yes")
        assert applied.exit_code == 0, applied.stdout
        name = QualifiedName(scope.database, scope.schema, Identifier.parse("ORDER_COUNT"))
        procedure = sql("CALL {procedure}()", procedure=qname(name))
        counted = live_connector.query(procedure).rows
        assert [str(row[0]) for row in counted] == ["2" if target == "a" else "3"]

    args = project_args(project, second_manifest)
    traced = run_sst("--output", "json", "lineage", *args, "tool:order_count", "--upstream")
    assert traced.exit_code == 0, traced.stdout
    assert {item["key"] for item in traced.envelope["data"]["nodes"]} == {"tool:order_count", "model:orders"}

    metrics = project / "semantic_models" / "metrics" / "live.yml"
    text = metrics.read_text(encoding="utf-8")
    metrics.write_text(text.replace("Total order value including tax.", "Order value, tax included."), encoding="utf-8")
    assert run_sst("--output", "json", "compile", *args, "--target", "b").exit_code == 0
    planned = run_sst("--output", "json", "plan", *args, "--target", "b", "--no-diff", "--no-plan-out")
    assert planned.exit_code == 2, planned.stdout
    [view] = [item for item in planned.envelope["data"]["changes"] if item["action"] == "update"]
    assert (view["diff_status"], view["changed_members"]) == ("from_state", ["metric:orders.total_revenue"])
