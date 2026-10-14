"""The view rules called directly: prose, tables, expressions and members."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from snowflake_semantic_tools.domain.model.authored import FilterDef, InstructionDef, MetricDef
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.resolve.membership import ViewMembers
from snowflake_semantic_tools.domain.validate.semantic.views import ViewInputs, bare_identifiers, view_rule_diagnostics
from tests.helpers.authored_documents import ORIGIN
from tests.helpers.semantic_members import BALANCES, MODELS, column

DESCRIBED = "Use this view for questions about orders."
EMPTY = DbtModel("model.t.empty", "empty", "DB.S.EMPTY", (), (), ())
ODD = DbtModel("model.t.odd", "odd", "DB.S.ODD", ("k",), (), (column("k", "NUMBER", "weird"),), has_contract=True)


def _view(name: str, tables: tuple[str, ...], **source: Any) -> ParsedView:
    return ParsedView(name, ORIGIN, "views.yml", {"description": DESCRIBED, **source}, tables)


def _inputs(kept: dict[str, set[str]], **fields: Any) -> ViewInputs:
    frozen = {view: frozenset(keys) for view, keys in kept.items()}
    defaults: dict[str, Any] = {
        "models": {**MODELS, "empty": EMPTY, "odd": ODD},
        "metrics": (),
        "filters": (),
        "relationships": (),
        "instructions": {},
        "members": ViewMembers(frozen, frozen),
    }
    return ViewInputs(**{**defaults, **fields})


def _codes(views: tuple[ParsedView, ...], inputs: ViewInputs) -> list[tuple[str, str | None]]:
    return [(item.code, item.subject) for item in view_rule_diagnostics(views, inputs)]


def test_a_view_is_described_for_routing_and_its_prose_stays_in_budget() -> None:
    instructions = {
        "long": InstructionDef("Long", "x" * 40, "y" * 40),
        "short": InstructionDef("Short", None, "Answer."),
        "unattached": InstructionDef("Unattached", "z" * 400, None),
    }
    views = (
        _view("terse", ("orders",), description="Orders."),
        _view("loud", ("orders",)),
        _view("quiet", ("orders",), description=""),
        replace(_view("broken", ("orders",), description="Orders."), poisoned=True),
    )
    kept = {"semantic_view:loud": {"custom_instruction:long", "custom_instruction:short"}}
    found = _codes(views, _inputs(kept, instructions=instructions, description_floor=30, instruction_budget=100))
    assert found == [
        ("SST-VAL005", "semantic_view:terse"),
        ("SST-VAL004", "semantic_view:terse"),
        ("SST-VAL018", "semantic_view:loud"),
    ]
    assert _codes(views[:1], _inputs({})) == [("SST-VAL005", "semantic_view:terse")]


def test_each_table_names_a_built_model_with_columns_and_a_contract_and_range_targets_declare_a_range() -> None:
    models = {
        **MODELS,
        "empty": EMPTY,
        "balances": replace(BALANCES, distinct_range=("as_of", "as_of")),
        "odd": replace(ODD, malformed_distinct_range=True),
    }
    view = _view("v", ("orders", "balances", "empty", "odd", "gone", "missing"))
    ranged = Relationship("ranged", "Orders", ("x",), "Balances", ("y",), range_bounds=("as_of", "as_of"))
    unranged = Relationship("unranged", "Balances", ("x",), "Empty", ("y",), range_bounds=("a", "b"))
    # A malformed range is SST-PRS028's to report on the model, not this view's.
    malformed = Relationship("malformed", "Orders", ("x",), "Odd", ("y",), range_bounds=("a", "b"))
    plain = Relationship("plain", "Orders", ("x",), "Empty", ("y",))
    kept = {"semantic_view:v": {f"relationship:{item.name}" for item in (ranged, unranged, malformed, plain)}}
    inputs = _inputs(
        kept, models=models, relationships=(ranged, unranged, malformed, plain), unavailable={"gone": "ephemeral"}
    )
    found = view_rule_diagnostics((view,), inputs)
    assert [(item.code, item.context.get("name")) for item in found] == [
        ("SST-VAL323", "empty"),
        ("SST-VAL324", "empty"),
        ("SST-VAL303", "gone"),
        ("SST-VAL206", "empty"),
    ]


def test_every_bare_word_a_view_attaches_is_a_column_or_a_variable_it_declares() -> None:
    assert bare_identifiers("SUM({{ ref('o', 'x') }}) + 'lit' + \"Q\" + total -- note\n + CAST(x AS NUMBER)") == (
        "total",
        "x",
    )
    metrics = (
        MetricDef("typo", "SUM(totl) * threshold", None, ()),
        MetricDef("fine", "SUM(total) * current_date", None, ()),
        MetricDef("refused", "SUM(total); DROP TABLE x", None, ()),
        MetricDef("unused_here", "SUM(unknown_word)", None, ()),
    )
    filters = (
        FilterDef("prose", "spare_word", None, ("orders",), False),
        FilterDef("entity", "{{ ref('orders', 'total') }} > threshold", None, ("orders",), True),
    )
    view = _view(
        "v",
        ("orders", "nowhere"),
        variables=[{"name": "threshold"}, {"name": "unused"}, "not a mapping", {"data_type": "NUMBER"}],
    )
    kept = {"semantic_view:v": {"metric:typo", "metric:fine", "metric:refused", "filter:prose", "filter:entity"}}
    found = view_rule_diagnostics((view,), _inputs(kept, metrics=metrics, filters=filters))
    assert [(item.code, item.context.get("identifier") or item.context.get("name")) for item in found] == [
        ("SST-VAL326", "totl"),
        ("SST-VAL221", "spare_word"),
        ("SST-VAL220", "unused"),
    ]


def test_a_view_needs_something_to_query() -> None:
    no_dimensions = replace(BALANCES, name="facts", columns=(column("balance", "NUMBER", "fact"),))
    models = {**MODELS, "facts": no_dimensions, "odd": ODD}
    views = (
        _view("bare", ("facts",)),
        _view("filtered", ("facts",)),
        _view("odd", ("odd",)),
        _view("unknown", ("nothing",)),
        _view(
            "hidden",
            ("orders",),
            exclude_columns=[
                f"{{{{ ref('orders', '{name}') }}}}" for name in ("order_id", "customer_id", "ordered_at", "state")
            ],
        ),
        _view("fine", ("orders",)),
    )
    entity = FilterDef("entity", "{{ ref('facts', 'balance') }} > 0", None, ("facts",), True)
    kept = {"semantic_view:filtered": {"filter:entity"}}
    found = _codes(views, _inputs(kept, models=models, filters=(entity,)))
    assert found == [
        ("SST-VAL301", "semantic_view:bare"),
        ("SST-VAL301", "semantic_view:hidden"),
    ]
