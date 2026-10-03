"""What each view attaches before it is built: the one attachment member resolution decides."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Origin
from snowflake_semantic_tools.domain.model.authored import InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.project import ParsedMember
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, ViewScope
from snowflake_semantic_tools.domain.parse.template import scan_template_calls
from snowflake_semantic_tools.domain.resolve.membership import view_members
from snowflake_semantic_tools.domain.resolve.membership_request import membership, membership_request
from tests.helpers.resolve_builders import VIEWS, member

ORIGIN = Origin("semantic_models/metrics/metrics.yml", 1, 3)


def _metric(name: str, expr: str, tables: tuple[str, ...] | None) -> ParsedMember:
    record = MetricDef(name, expr, None, (), tables or (), has_tables_key=tables is not None, origin=ORIGIN)
    return ParsedMember("metric", name, ORIGIN, record, tables, scan_template_calls(expr))


def test_a_view_attaches_by_tables_by_name_and_through_the_metrics_a_metric_reads() -> None:
    base = _metric("revenue", "SUM({{ ref('orders', 'total') }})", ("orders",))
    derived = _metric("double_revenue", "{{ metric('revenue') }} * 2", None)
    tableless = _metric("constant", "COUNT(fudge)", None)
    instruction = ParsedMember("custom_instruction", "Tone", ORIGIN, InstructionDef("Tone", "Be brief.", None), None)
    attached = view_members(
        membership_request(
            (base, derived, tableless, instruction),
            frozenset(),
            view_tables=VIEWS,
            view_instructions={"semantic_view:sales": frozenset(("tone",))},
        )
    )
    assert attached.reached["semantic_view:sales"] == {
        "metric:revenue",
        "metric:double_revenue",
        "custom_instruction:tone",
    }
    assert attached.reached["semantic_view:menu"] == {"metric:revenue", "metric:double_revenue"}
    # A metric that names no table and reads no metric attaches to no view.
    assert not attached.reaches("semantic_view:sales", "metric:constant")
    assert attached.poisoned == frozenset()


def test_a_scope_narrows_what_is_kept_but_not_what_is_reached() -> None:
    kept = member("metric", "kept", ("orders",))
    dropped = member("metric", "dropped", ("orders",))
    attached = view_members(
        membership_request(
            (kept, dropped),
            frozenset(),
            view_tables=VIEWS,
            view_instructions={},
            view_scopes={"semantic_view:sales": ViewScope(metrics=("KEPT",))},
        )
    )
    assert attached.reaches("semantic_view:sales", "metric:dropped")
    assert not attached.keeps("semantic_view:sales", "metric:dropped")
    assert attached.keeps("semantic_view:sales", "metric:kept")
    assert attached.keeps("semantic_view:menu", "metric:dropped")
    # A view the attachment never names keeps nothing.
    assert not attached.keeps("semantic_view:unknown", "metric:kept")


def test_a_poisoned_member_reaches_no_view_and_is_listed_as_left_out() -> None:
    healthy = member("metric", "healthy", ("orders",))
    broken = member("metric", "broken", ("orders",))
    attached = view_members(
        membership_request((healthy, broken), frozenset(("metric:broken",)), view_tables=VIEWS, view_instructions={})
    )
    assert attached.poisoned == {"metric:broken"}
    assert attached.reached["semantic_view:sales"] == {"metric:healthy"}


def test_the_request_reads_each_records_facts_joins_and_channels() -> None:
    metric = ParsedMember(
        "metric",
        "m",
        ORIGIN,
        MetricDef(
            "m",
            "{{ metric('n') }}",
            None,
            (),
            (),
            derived=True,
            using_relationships=("JOIN",),
            access_modifier="private_access",
        ),
        None,
        scan_template_calls("{{ metric('n') }}"),
    )
    query = ParsedMember(
        "verified_query",
        "q",
        ORIGIN,
        VerifiedQueryDef("q", "How many?", "SELECT {{ metric('m') }}", ("orders",), None, None, None),
        ("orders",),
        scan_template_calls("SELECT {{ metric('m') }}"),
    )
    join = ParsedMember(
        "relationship",
        "join",
        ORIGIN,
        Relationship("join", "Orders", ("customer_id",), "Customers", ("id",)),
        ("orders", "customers"),
    )
    instruction = ParsedMember("custom_instruction", "Tone", ORIGIN, InstructionDef("Tone", None, "Route."), None)
    reported = (
        D("SST-REF001", subject="metric:M", model="x"),
        D("SST-VAL001", subject="metric:m"),
    )
    request = membership_request(
        (metric, query, join, instruction),
        frozenset(("metric:m",)),
        view_tables=VIEWS,
        view_instructions={},
        reported=reported,
    )
    assert request.facts["metric:m"].derived and request.facts["metric:m"].private
    assert request.facts["metric:m"].using_relationships == ("join",)
    assert request.facts["verified_query:q"].referenced_metrics == ("m",)
    assert "relationship:join" not in request.facts
    assert request.joins == (("orders", "customers"),)
    assert request.instruction_channels == {"tone": frozenset(("ai_question_categorization",))}
    assert request.unresolved == {"metric:m": 1}
    marked, result = membership(
        (metric, query),
        frozenset(),
        view_tables=VIEWS,
        reported_views=frozenset(VIEWS),
        view_instructions={},
        known_models=frozenset(("orders",)),
        reported=(),
    )
    assert [item.poisoned for item in marked] == [False, False]
    assert result.attachment["verified_query:q"] == ("semantic_view:menu", "semantic_view:sales")
