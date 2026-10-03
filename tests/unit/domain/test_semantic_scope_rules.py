"""A view's scope lists read and checked directly: entry forms, shapes, modes, and what kept metrics need."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.model.authored import MetricDef, NonAdditiveDef, WindowDef, WindowOrderDef
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.resolve.membership import ViewMembers
from snowflake_semantic_tools.domain.validate.semantic.scope import scope_diagnostics, scope_entries, view_scope
from tests.helpers.authored_documents import ORIGIN
from tests.helpers.semantic_members import MODELS, column

NOTES = DbtModel("model.t.notes", "notes", "DB.S.NOTES", ("note_id",), (), (column("note_id", "NUMBER", None),))
ALL_MODELS = {**MODELS, "notes": NOTES}
JOINED = Relationship("joined", "Orders", ("customer_id",), "Balances", ("account_id",))
AWAY = Relationship("away", "orders", ("x",), "elsewhere", ("y",))


def _view(
    name: str = "v", tables: tuple[str, ...] = ("orders", "balances", "notes", "ghost"), **source: Any
) -> ParsedView:
    return ParsedView(name, ORIGIN, "views.yml", source, tables)


def _members(
    reached: dict[str, set[str]], kept: dict[str, set[str]] | None = None, poisoned: frozenset[str] = frozenset()
) -> ViewMembers:
    def frozen(mapping: dict[str, set[str]]) -> dict[str, frozenset[str]]:
        return {view: frozenset(keys) for view, keys in mapping.items()}

    return ViewMembers(frozen(reached), frozen(kept if kept is not None else reached), frozenset(poisoned))


def test_each_scope_entry_is_read_in_its_kinds_form_or_kept_as_written() -> None:
    node = {
        "columns": ["{{ ref('Orders', 'State') }}", "{{ ref('orders') }}", "state", "{{ ref('orders'"],
        "exclude_metrics": ["{{ metric('Total') }}", "Revenue", "{{ metric('a', 'b') }}", "{{ metric('x'"],
        "relationships": ["joined", "a.b", " "],
        "exclude_relationships": "not a list",
    }
    entries = scope_entries(node)
    assert [(entry.field, entry.label, entry.readable) for entry in entries] == [
        ("columns", "orders.state", True),
        ("columns", "{{ ref('orders') }}", False),
        ("columns", "state", False),
        ("columns", "{{ ref('orders'", False),
        ("exclude_metrics", "total", True),
        ("exclude_metrics", "revenue", True),
        ("exclude_metrics", "{{ metric('a', 'b') }}", False),
        ("exclude_metrics", "{{ metric('x'", False),
        ("relationships", "joined", True),
        ("relationships", "a.b", False),
        ("relationships", "", False),
    ]
    scope = view_scope(node)
    assert scope.columns == ("ORDERS.STATE",) and scope.metrics is None
    assert scope.exclude_metrics == ("TOTAL", "REVENUE") and scope.relationships == ("JOINED",)


def test_a_scope_names_only_members_the_view_provides_in_one_mode_per_kind() -> None:
    views = (
        _view(
            columns=[
                "{{ ref('orders', 'state') }}",
                "{{ ref('orders', 'secret') }}",
                "{{ ref('items', 'x') }}",
                "{{ ref('ghost', 'x') }}",
                "{{ ref('orders', 'nothing') }}",
                "{{ ref('notes', 'note_id') }}",
                "state",
            ],
            exclude_columns=["{{ ref('orders', 'secret') }}"],
            metrics=["total", "gone", "unattached", "broken", "x.y"],
            relationships=["joined", "missing", "away"],
            exclude_relationships="joined",
        ),
        ParsedView("p", ORIGIN, "views.yml", {"columns": "x"}, (), poisoned=True),
    )
    metrics = (
        MetricDef("total", "1", None, ()),
        MetricDef("unattached", "1", None, ()),
        MetricDef("broken", "1", None, ()),
    )
    members = _members({"semantic_view:v": {"metric:total"}}, poisoned=frozenset(("metric:broken",)))
    found = scope_diagnostics(views, metrics, (JOINED, AWAY), ALL_MODELS, members)
    assert [
        (item.code, item.context.get("name") or item.context.get("field"), item.context.get("reason")) for item in found
    ] == [
        ("SST-PRS003", "exclude_relationships", None),
        ("SST-VAL329", "columns", None),
        ("SST-VAL329", "relationships", None),
        ("SST-VAL330", "orders.secret", "is excluded globally, so no view can include it"),
        ("SST-VAL330", "items.x", "is not on a table of this view"),
        ("SST-VAL330", "ghost.x", "does not exist on 'ghost'"),
        ("SST-VAL330", "orders.nothing", "does not exist on 'orders'"),
        ("SST-VAL330", "notes.note_id", "declares no column_type, so it is not a member"),
        ("SST-VAL330", "state", "is not a two-argument ref() call"),
        ("SST-VAL331", None, None),
        ("SST-VAL330", "gone", "does not exist"),
        ("SST-VAL330", "unattached", "does not attach to this view"),
        ("SST-VAL330", "x.y", "is not a metric name"),
        ("SST-VAL330", "missing", "does not exist"),
        ("SST-VAL203", "elsewhere", None),
    ]


def test_a_kept_metric_keeps_the_relationships_and_dimension_columns_it_needs() -> None:
    metric = MetricDef(
        "balance",
        "SUM({{ ref('balances', 'balance') }})",
        None,
        (),
        ("balances",),
        using_relationships=("JOINED", "AWAY", "UNDECLARED"),
        non_additive=(NonAdditiveDef("as_of"), NonAdditiveDef("ordered_at", "orders")),
        window=WindowDef(
            partition_by=("{{ ref('balances', 'account_id') }}", "{{ ref('balances'", "{{ ref('balances') }}"),
            order_by=(WindowOrderDef("{{ metric('total') }}"),),
        ),
    )
    loose = MetricDef("loose", "COUNT(*)", None, (), non_additive=(NonAdditiveDef("as_of"),))
    dropped = MetricDef("dropped", "1", None, (), using_relationships=("AWAY",))
    view = _view(
        exclude_relationships=["joined"],
        exclude_columns=["{{ ref('balances', 'as_of') }}", "{{ ref('balances', 'account_id') }}"],
    )
    members = _members(
        {"semantic_view:v": {"metric:balance", "metric:loose", "metric:dropped"}},
        {"semantic_view:v": {"metric:balance", "metric:loose"}},
    )
    found = scope_diagnostics((view,), (metric, loose, dropped), (JOINED, AWAY), ALL_MODELS, members)
    assert [(item.code, item.context.get("relationship") or item.context.get("column")) for item in found] == [
        ("SST-VAL332", "joined"),
        ("SST-VAL332", "away"),
        ("SST-VAL318", "balances.as_of"),
        ("SST-VAL318", "balances.account_id"),
    ]
    unscoped = scope_diagnostics((_view(),), (loose,), (), ALL_MODELS, _members({"semantic_view:v": {"metric:loose"}}))
    assert unscoped == ()
