"""The semantic member rules called directly: instructions, join conditions and keys, fidelity, 0.3 keys, windows."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.authored import InstructionDef, MetricDef, WindowDef, WindowOrderDef
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.validate.semantic.deprecated import deprecated_key_diagnostics, honoured
from snowflake_semantic_tools.domain.validate.semantic.fidelity import renderer_fidelity_diagnostics, verifier_change
from snowflake_semantic_tools.domain.validate.semantic.instructions import (
    contradiction_diagnostics,
    instruction_diagnostics,
)
from snowflake_semantic_tools.domain.validate.semantic.joins import (
    dropped_condition,
    is_self_loop,
    key_diagnostic,
    unparsable_condition,
)
from snowflake_semantic_tools.domain.validate.semantic.nodes import member_root
from snowflake_semantic_tools.domain.validate.semantic.windows import metric_owner, window_diagnostics
from tests.helpers.authored_documents import ORIGIN, document, documents
from tests.helpers.semantic_members import BALANCES, MODELS, ORDERS


def _codes(found: object) -> list[str]:
    return [item.code for item in found]  # type: ignore[attr-defined]


def test_each_instruction_has_text_in_the_channel_that_acts_on_it_and_no_state_words() -> None:
    instructions = (
        InstructionDef("Empty", None, None, origin=ORIGIN),
        InstructionDef("Renamed", None, None, origin=ORIGIN, renamed=True),
        InstructionDef("Sql", "Decline to answer off-topic questions.", "Round to cents in the SQL.", origin=ORIGIN),
        InstructionDef("States", "Return UNCLEAR or UNCLEAR when AMBIGUOUS.", None, origin=ORIGIN),
        InstructionDef("Fine", "Use the order date.", "Answer order questions.", origin=ORIGIN),
    )
    found = instruction_diagnostics(instructions)
    assert [(item.code, item.context["member"]) for item in found] == [
        ("SST-VAL407", "Empty"),
        ("SST-VAL409", "Sql"),
        ("SST-VAL409", "Sql"),
        ("SST-VAL411", "States"),
    ]
    assert found[3].context["found"] == "UNCLEAR, AMBIGUOUS"


def test_two_instructions_a_view_attaches_contradict_when_one_forbids_what_the_other_directs() -> None:
    instructions = {
        "always": InstructionDef("Always", "Always round to cents.", None, origin=ORIGIN),
        "never": InstructionDef("Never", None, "Never round to cents!", origin=ORIGIN),
        "loose": InstructionDef("Loose", "Never round to cents.", None),
        "other": InstructionDef("Other", "Always use UTC.", None),
    }
    views = {
        "semantic_view:v": frozenset(("always", "never", "missing", "other")),
        "semantic_view:w": frozenset(("never", "loose")),
    }
    found = contradiction_diagnostics(instructions, views)
    assert [(item.subject, item.context["a"], item.context["b"], item.related) for item in found] == [
        ("semantic_view:v", "Always", "Never", (ORIGIN,)),
    ]
    reversed_origin = contradiction_diagnostics(
        {"a": InstructionDef("A", "Always x.", None), "b": InstructionDef("B", "Never x.", None, origin=ORIGIN)},
        {"semantic_view:v": frozenset(("a", "b"))},
    )
    assert [item.related for item in reversed_origin] == [()]


def test_a_condition_is_multi_column_or_unparsable_and_only_one_range_or_asof_renders() -> None:
    two = "{{ ref('a', 'x') }} || {{ ref('a', 'y') }} = {{ ref('b', 'z') }}"
    assert unparsable_condition(two, relationship="r", origin=ORIGIN, subject="relationship:r").code == "SST-VAL213"
    assert (
        unparsable_condition("a LIKE b", relationship="r", origin=ORIGIN, subject="relationship:r").code == "SST-PRS110"
    )
    assert (
        unparsable_condition("{{ ref('a', 'x') }} = 1", relationship="r", origin=ORIGIN, subject="s").code
        == "SST-PRS110"
    )
    conditions = ("c0", "c1", "c2")

    def dropped(*kinds: str) -> object:
        found = dropped_condition(kinds, conditions, relationship="r", origin=ORIGIN, subject="s")
        return found.context["value"] if found is not None else None

    assert dropped("equality", "range", "equality") == "c0"
    assert dropped("asof", "equality", "asof") == "c0"
    assert dropped("range") is None
    assert dropped("equality", "asof") is None


def test_a_join_target_should_hold_one_row_per_joined_key() -> None:
    def key(to_columns: tuple[str, ...], target: object) -> str | None:
        relationship = Relationship("R", "Orders", ("x",) * len(to_columns), "Balances", to_columns)
        found = key_diagnostic(relationship, target, subject="relationship:r", origin=ORIGIN)  # type: ignore[arg-type]
        return found.code if found else None

    keyless = replace(BALANCES, primary_key=())
    assert key(("account_id",), keyless) == "SST-VAL311"
    assert key(("ACCOUNT_ID", "as_of"), BALANCES) is None
    assert key(("account_id",), BALANCES) == "SST-VAL208"
    assert key(("balance",), BALANCES) == "SST-VAL210"
    assert key(("order_id",), ORDERS) is None
    assert is_self_loop(Relationship("R", "Orders", ("a",), "ORDERS", ("b",)))
    assert not is_self_loop(Relationship("R", "Orders", ("a",), "Balances", ("b",)))


def test_a_value_the_renderer_drops_or_changes_is_reported() -> None:
    filters, queries = member_root("filter"), member_root("verified_query")
    tree = {
        filters: [
            {"name": "f", "labels": ["filter", "Hidden", 3]},
            {"name": "g", "labels": "filter"},
            {"labels": ["x"]},
        ],
        queries: [
            {"name": "q", "verified_by": " ann "},
            {"name": "r", "verified_by": "ann"},
            {"verified_by": 0},
        ],
    }
    file = document("m.yml", tree, ((filters, 0), 3), ((queries, 0), 9))
    found = renderer_fidelity_diagnostics(documents(file))
    assert [(item.subject, item.context["field"], item.context["detail"], item.origin.line) for item in found] == [  # type: ignore[union-attr]
        ("filter:f", "labels: Hidden", "dropped", 3),
        ("verified_query:q", "verified_by", "trimmed", 9),
    ]
    assert [verifier_change({"verified_by": value}) for value in (None, 7, 0, "  ", "x")] == [
        None,
        "converted to text",
        "dropped",
        "dropped",
        None,
    ]


def test_a_0_3_spelling_is_honoured_alone_and_refused_beside_its_1_0_key() -> None:
    assert honoured({"access_modifier": "x"}, "access_modifier", "metric") == "x"
    assert honoured({"visibility": " Private "}, "access_modifier", "metric") == "private_access"
    assert honoured({"visibility": "odd"}, "access_modifier", "metric") == "odd"
    assert honoured({"sql_generation": "s"}, "ai_sql_generation", "custom_instruction") == "s"
    assert honoured({}, "expr", "filter") is None
    instructions, metrics, relationships = (
        member_root(name) for name in ("custom_instruction", "metric", "relationship")
    )
    tree = {
        instructions: [
            {"name": "i", "sql_generation": "s"},
            {"name": "j", "sql_generation": "s", "ai_sql_generation": "t"},
        ],
        metrics: [{"name": "m", "visibility": "private"}, {"visibility": "x"}, "x"],
        relationships: [
            {"name": "r", "join_type": "inner", "relationship_type": "many_to_one"},
            {"name": "s", "join_type": "x"},
        ],
    }
    file = document("m.yml", tree, ((instructions, 0, "sql_generation"), 4))
    lists = document("n.yml", {metrics: "not a list"})
    found = deprecated_key_diagnostics(documents(file, lists))
    assert [(item.code, item.subject, item.origin.line) for item in found] == [  # type: ignore[union-attr]
        ("SST-VAL012", "custom_instruction:i", 4),
        ("SST-PRS020", "custom_instruction:j", None),
        ("SST-VAL122", "metric:m", None),
        ("SST-VAL211", "relationship:r", None),
        ("SST-VAL211", "relationship:r", None),
        ("SST-VAL211", "relationship:s", None),
    ]


def _window_metric(expr: str, window: WindowDef, tables: tuple[str, ...] = ("orders",)) -> MetricDef:
    return MetricDef("w", expr, None, (), tables, window=window, origin=ORIGIN)


def test_a_window_metric_applies_to_an_aggregate_on_one_table_over_dimensions_or_sibling_metrics() -> None:
    sibling = MetricDef("total", "SUM({{ ref('orders', 'total') }})", None, (), ("orders",))
    derived = MetricDef("derived", "{{ metric('total') }}", None, (), (), derived=True)
    windowed = _window_metric("SUM(1)", WindowDef())
    elsewhere = MetricDef("other", "SUM(1)", None, (), ("balances",))
    metrics = {"total": sibling, "derived": derived, "windowed": windowed, "other": elsewhere}
    window = WindowDef(
        partition_by=("{{ ref('orders', 'state') }}", "{{ ref('orders', 'total') }}", "{{ ref('orders', 'secret') }}"),
        partition_excluding=("{{ metric('total') }}",),
        order_by=(
            WindowOrderDef("{{ metric('total') }}"),
            WindowOrderDef("{{ metric('derived') }}"),
            WindowOrderDef("{{ metric('windowed') }}"),
            WindowOrderDef("{{ metric('other') }}"),
            WindowOrderDef("{{ metric('missing') }}"),
            WindowOrderDef("{{ ref('nowhere', 'x') }}"),
            WindowOrderDef("{{ ref('orders', 'nothing') }}"),
            WindowOrderDef("{{ ref('orders'"),
            WindowOrderDef("ordered_at"),
        ),
    )
    found = window_diagnostics(_window_metric("RANK(SUM({{ ref('orders', 'total') }}))", window), metrics, MODELS)
    assert [(item.code, item.context.get("field")) for item in found] == [
        ("SST-VAL129", "partition_by[1]"),
        ("SST-VAL129", "partition_by[2]"),
        ("SST-VAL129", "partition_by_excluding[0]"),
        ("SST-VAL129", "order_by[1]"),
        ("SST-VAL129", "order_by[2]"),
        ("SST-VAL129", "order_by[3]"),
        ("SST-VAL129", "order_by[4]"),
        ("SST-VAL129", "order_by[5]"),
        ("SST-VAL129", "order_by[6]"),
        ("SST-VAL129", "order_by[7]"),
        ("SST-VAL129", "order_by[8]"),
    ]
    framed = WindowDef(frame="ROWS BETWEEN 1 PRECEDING AND CURRENT ROW", has_frame=True)
    raw = window_diagnostics(_window_metric("LAG(total)", framed, ()), metrics, MODELS)
    assert [(item.code, item.context.get("function")) for item in raw] == [
        ("SST-VAL126", "LAG"),
        ("SST-VAL102", "LAG"),
        ("SST-VAL127", None),
    ]
    spread = window_diagnostics(_window_metric("1 + 1", WindowDef(), ("orders", "balances")), metrics, MODELS)
    assert [(item.code, item.context["function"]) for item in spread] == [
        ("SST-VAL126", "the expression"),
        ("SST-VAL102", "window"),
    ]
    ordered = WindowDef(
        order_by=(WindowOrderDef("{{ ref('orders', 'ordered_at') }}"),),
        frame="ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
    )
    assert window_diagnostics(_window_metric("SUM(SUM({{ ref('orders', 'total') }}))", ordered), metrics, MODELS) == []
    assert metric_owner(MetricDef("m", "{{ ref('balances', 'balance') }}", None, ())) == "balances"
