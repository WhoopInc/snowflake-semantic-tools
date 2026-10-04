"""Member resolution: the attachment, the checks on what it means, and the checks on built views."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    Table,
    VerifiedQuery,
    ViewScope,
)
from snowflake_semantic_tools.domain.resolve.membership import invariant_diagnostics, resolve_membership
from snowflake_semantic_tools.domain.resolve.membership_model import NO_FACTS, MemberFacts
from snowflake_semantic_tools.domain.resolve.rendered import rendered_diagnostics
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership, request
from tests.helpers.sql_values import authored, authored_query


def _codes(diagnostics: Iterable[Diagnostic]) -> list[str]:
    return [item.code for item in diagnostics]


def test_a_healthy_project_attaches_by_tables_and_reports_only_what_attached_where() -> None:
    metric = member("metric", "m", ("orders",), "COUNT({{ ref('orders', 'id') }})")
    menu_only = member("metric", "p", ("products",))
    dimension = member("dimension", "orders.id", ("orders",))
    result = membership(metric, menu_only, dimension)
    assert result.attachment[metric.key] == ("semantic_view:menu", "semantic_view:sales")
    assert result.attachment[menu_only.key] == ("semantic_view:menu",)
    assert _codes(result.diagnostics) == ["SST-MEM011", "SST-MEM103", "SST-MEM103"]
    # A fact or dimension's reach follows its table, so it is not reported as fan-out.
    assert coded(result.diagnostics, "SST-MEM011")[0].subject == "metric:m"


def test_a_view_scope_narrows_the_metrics_and_relationships_it_attaches() -> None:
    kept = member("metric", "kept", ("orders",))
    left_out = member("metric", "left_out", ("orders",))
    join = member("relationship", "orders_to_customers", ("orders", "customers"))
    dimension = member("dimension", "orders.id", ("orders",))
    scopes = {
        "semantic_view:menu": ViewScope(metrics=("KEPT",)),
        "semantic_view:sales": ViewScope(exclude_metrics=("KEPT",), exclude_relationships=("ORDERS_TO_CUSTOMERS",)),
    }
    result = membership(kept, left_out, join, dimension, view_scopes=scopes)
    assert result.attachment[kept.key] == ("semantic_view:menu",)
    assert result.attachment[left_out.key] == ("semantic_view:sales",)
    assert result.attachment[join.key] == ()
    # A scope lists metrics and relationships; a dimension still follows its table.
    assert result.attachment[dimension.key] == ("semantic_view:menu", "semantic_view:sales")
    assert coded(result.diagnostics, "SST-MEM013") == []


def test_facts_default_and_metric_dependencies_follow_metric_references() -> None:
    query = member("verified_query", "q", ("orders",))
    facts = {
        "metric:a": MemberFacts(referenced_metrics=("b",)),
        "verified_query:q": MemberFacts(referenced_metrics=("x",)),
    }
    built = request(query, facts=facts)
    assert built.facts_of(member("metric", "other")) is NO_FACTS
    assert built.metric_dependencies() == {"metric:a": ("metric:b",)}


def test_table_checks_skip_poisoned_view_named_and_column_members() -> None:
    poisoned = member("metric", "bad", ("orders", "orders"), poisoned=True)
    instruction = member("custom_instruction", "rules")
    column = member("fact", "orders.amount", ("orders",))
    result = membership(poisoned, instruction, column)
    assert coded(result.diagnostics, "SST-MEM004") == []
    assert coded(result.diagnostics, "SST-MEM005") == []


def test_an_unknown_table_is_left_to_the_known_model_check() -> None:
    result = membership(member("metric", "m", ("nowhere", "suppliers", "suppliers")))
    assert [item.context["name"] for item in coded(result.diagnostics, "SST-MEM001")] == ["suppliers"]
    assert [item.context["name"] for item in coded(result.diagnostics, "SST-MEM004")] == ["suppliers"]


def test_a_derived_metric_is_neither_unlisted_nor_uninferable_nor_composed() -> None:
    derived = member("metric", "d", None, "{{ metric('m') }}")
    base = member("metric", "m", ("customers",))
    facts = {"metric:d": MemberFacts(derived=True, referenced_metrics=("m",))}
    result = membership(derived, base, facts=facts)
    codes = _codes(result.diagnostics)
    assert "SST-MEM002" not in codes and "SST-MEM006" not in codes and "SST-MEM007" not in codes
    assert result.attachment[derived.key] == ("semantic_view:sales",)


def test_transitive_tables_follow_chains_once_and_stop_at_unknown_metrics() -> None:
    top = member("metric", "top", ("orders",))
    middle = member("metric", "middle", ("orders",))
    leaf = member("metric", "leaf", None, "COUNT({{ ref('customers', 'id') }})")
    facts = {
        "metric:top": MemberFacts(referenced_metrics=("middle", "ghost")),
        "metric:middle": MemberFacts(referenced_metrics=("leaf", "top")),
    }
    result = membership(top, middle, leaf, facts=facts)
    reached = {item.subject: item.context["outside"] for item in coded(result.diagnostics, "SST-MEM007")}
    assert reached == {"metric:top": "customers", "metric:middle": "customers"}


def test_an_unattached_member_built_on_a_poisoned_metric_is_not_reported() -> None:
    broken = member("metric", "broken", ("orders",), poisoned=True)
    derived = member("metric", "derived", None)
    built_on_derived = member("metric", "again", None)
    facts = {
        "metric:derived": MemberFacts(derived=True, referenced_metrics=("broken",)),
        "metric:again": MemberFacts(derived=True, referenced_metrics=("derived",)),
    }
    result = membership(broken, derived, built_on_derived, facts=facts)
    assert coded(result.diagnostics, "SST-MEM005") == []


def test_an_unattached_derived_metric_is_reported() -> None:
    derived = member("metric", "derived", None)
    base = member("metric", "base", ("suppliers",))
    facts = {"metric:derived": MemberFacts(derived=True, referenced_metrics=("base",))}
    result = membership(derived, base, facts=facts)
    assert [item.subject for item in coded(result.diagnostics, "SST-MEM005")] == ["metric:derived", "metric:base"]


def test_a_join_only_table_is_named_only_when_a_relationship_reaches_it() -> None:
    missing_two = member("metric", "m", ("suppliers", "customers", "products"))
    joins = (("customers", "orders"), ("orders", "products"))
    result = membership(missing_two, joins=joins, views={"semantic_view:menu": frozenset(("orders", "products"))})
    assert [item.context["name"] for item in coded(result.diagnostics, "SST-MEM010")] == ["customers"]
    lonely = membership(member("metric", "m", ("suppliers",)), views={})
    assert coded(lonely.diagnostics, "SST-MEM010") == []


def test_a_verified_query_naming_a_public_metric_or_none_is_quiet() -> None:
    query = member("verified_query", "q", ("orders",))
    public = member("metric", "revenue", ("orders",))
    facts = {"verified_query:q": MemberFacts(referenced_metrics=("revenue", "elsewhere"))}
    assert coded(membership(query, public, facts=facts).diagnostics, "SST-MEM015") == []


def test_conflicting_scope_needs_a_view_with_the_join_and_one_without() -> None:
    relationship = member("relationship", "orders_to_customers", ("orders", "customers"))
    nowhere = member("metric", "m", ("suppliers",))
    joined = MemberFacts(using_relationships=("orders_to_customers",))
    assert coded(membership(nowhere, relationship, facts={"metric:m": joined}).diagnostics, "SST-MEM016") == []
    everywhere = member("metric", "e", ("orders",))
    unjoined = MemberFacts(using_relationships=("nothing",))
    assert coded(membership(everywhere, facts={"metric:e": unjoined}).diagnostics, "SST-MEM016") == []


def test_a_poisoned_member_is_reported_skipped_only_for_unresolved_references() -> None:
    poisoned = member("metric", "m", ("orders",), poisoned=True)
    healthy = member("metric", "h", ("orders",))
    result = membership(poisoned, healthy, unresolved={"metric:h": 3})
    assert coded(result.diagnostics, "SST-MEM107") == []


def test_view_contents_skip_unreported_views_and_views_on_unknown_models() -> None:
    views = {"semantic_view:menu": frozenset(("orders",)), "semantic_view:odd": frozenset(("nowhere",))}
    metric = member("metric", "m", ("nowhere",))
    result = membership(metric, views=views, reported_views=frozenset(("semantic_view:odd",)))
    assert coded(result.diagnostics, "SST-MEM104") == []
    assert [item.message for item in coded(result.diagnostics, "SST-MEM103")] == ["semantic_view:odd: 1 metric"]
    empty = membership(views=views, reported_views=frozenset(("semantic_view:menu",)))
    assert [item.message for item in coded(empty.diagnostics, "SST-MEM103")] == ["semantic_view:menu: no members"]


def test_instruction_conflicts_read_each_channel_and_need_a_shared_table() -> None:
    sql = frozenset(("ai_sql_generation",))
    scope = frozenset(("ai_question_categorization",))
    channels = {"a": sql, "b": scope}
    instructions = (
        member("custom_instruction", "a"),
        member("custom_instruction", "b"),
        member("metric", "m", ("orders",)),
    )
    split = {"semantic_view:menu": frozenset({"a"}), "semantic_view:sales": frozenset({"b"})}
    result = membership(*instructions, view_named_members=split, instruction_channels=channels)
    assert coded(result.diagnostics, "SST-MEM105") == []
    apart = {"semantic_view:menu": frozenset(("products",)), "semantic_view:sales": frozenset(("customers",))}
    both = {"semantic_view:menu": frozenset({"a"}), "semantic_view:sales": frozenset({"a"})}
    unshared = membership(*instructions, views=apart, view_named_members=both, instruction_channels={"a": sql})
    assert coded(unshared.diagnostics, "SST-MEM105") == []
    unknown = membership(*instructions, view_named_members=split)
    assert coded(unknown.diagnostics, "SST-MEM105") == []


def test_invariants_hold_for_view_named_dependent_and_unknown_artifacts() -> None:
    instruction = member("custom_instruction", "rules")
    derived = member("metric", "d", None)
    base = member("metric", "b", ("orders",))
    facts = {"metric:d": MemberFacts(derived=True, referenced_metrics=("b",))}
    built = request(instruction, derived, base, facts=facts)
    placed = {instruction.key: ("semantic_view:menu",), derived.key: ("semantic_view:sales",), base.key: ("x:y",)}
    found = invariant_diagnostics(built, {base.key: ("x:y",)}, placed, placed)
    assert [(item.code, item.subject) for item in found] == [("SST-MEM100", "metric:b")]
    # The real attachment of the same members contradicts nothing.
    assert invariant_diagnostics(built, *(resolve_membership(built).attachment,) * 3) == ()


def _view(**fields: Any) -> SemanticView:
    base: dict[str, Any] = {
        "fqn": "DB.SCH.SALES",
        "tables": (Table(logical_name="ORDERS", fqn="DB.SCH.ORDERS", synonyms=("sale", "Sale")),),
    }
    return SemanticView(**{**base, **fields})


def test_rendered_views_hold_every_attached_member_type() -> None:
    view = _view(
        columns=(Column("ORDERS", "IS_BIG", ColumnKind.FILTER, authored("ORDERS.AMOUNT > 1")),),
        metrics=(Metric("REVENUE", authored("SUM(ORDERS.AMOUNT)"), table="ORDERS"),),
        relationships=(Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("ID",), "CUSTOMERS", ("ID",)),),
        verified_queries=(VerifiedQuery("Q", "How many?", authored_query("SELECT 1")),),
        custom_instruction_names=("RULES",),
        ai_sql_generation="For ORDERS, threshold is 5.",
    )
    members = (
        member("metric", "revenue"),
        member("filter", "is_big"),
        member("filter", "threshold"),
        member("relationship", "orders_to_customers"),
        member("verified_query", "q"),
        member("custom_instruction", "rules"),
        member("dimension", "orders.id"),
        member("metric", "elsewhere"),
    )
    key = "semantic_view:sales"
    attachment = {item.key: (key,) for item in members[:-1]}
    assert rendered_diagnostics({key: view}, members, attachment) == ()
    missing = rendered_diagnostics({key: _view()}, members[:1], attachment)
    assert [item.code for item in missing] == ["SST-MEM014"]


def test_synonym_clashes_involve_a_metric_and_count_each_claimant_once() -> None:
    clash = _view(
        columns=(
            Column("ORDERS", "ID", ColumnKind.DIMENSION, authored("ORDERS.ID"), synonyms=("sale",)),
            Column("ORDERS", "KEY", ColumnKind.DIMENSION, authored("ORDERS.KEY"), synonyms=("key",)),
        ),
        metrics=(
            Metric("REVENUE", authored("SUM(ORDERS.AMOUNT)"), table="ORDERS", synonyms=("key", "money")),
            Metric("TOTAL", authored("ORDERS.REVENUE"), synonyms=("money",)),
        ),
    )
    found = rendered_diagnostics({"semantic_view:sales": clash}, (), {})
    assert [(item.context["a"], item.context["b"]) for item in found] == [
        ("ORDERS.KEY", "ORDERS.REVENUE"),
        ("ORDERS.REVENUE", "TOTAL"),
    ]


SQL = frozenset(("ai_sql_generation",))
FIRES: dict[str, Any] = {
    "SST-MEM001": lambda: membership(member("metric", "m", ("suppliers",))),
    "SST-MEM002": lambda: membership(member("filter", "f", ())),
    "SST-MEM006": lambda: membership(member("metric", "m", ("orders",)), facts={"metric:m": MemberFacts(derived=True)}),
    "SST-MEM011": lambda: membership(member("metric", "m", ("orders",))),
    "SST-MEM012": lambda: membership(member("metric", "m", None, "COUNT({{ ref('orders', 'id') }})")),
    "SST-MEM101": lambda: membership(member("metric", "m", ("orders",), "COUNT({{ ref('customers', 'id') }})")),
    "SST-MEM107": lambda: membership(member("metric", "m", ("orders",), poisoned=True), unresolved={"metric:m": 1}),
    "SST-MEM015": lambda: membership(
        member("metric", "secret", ("orders",)),
        member("verified_query", "q", ("orders",)),
        facts={
            "metric:secret": MemberFacts(private=True),
            "verified_query:q": MemberFacts(referenced_metrics=("secret",)),
        },
    ),
    "SST-MEM016": lambda: membership(
        member("metric", "m", ("orders",)),
        member("relationship", "orders_to_customers", ("orders", "customers")),
        facts={"metric:m": MemberFacts(using_relationships=("orders_to_customers",))},
    ),
    "SST-MEM105": lambda: membership(
        member("custom_instruction", "a"),
        member("custom_instruction", "b"),
        view_named_members={"semantic_view:menu": frozenset({"a"}), "semantic_view:sales": frozenset({"b"})},
        instruction_channels={"a": SQL, "b": SQL},
    ),
}


@pytest.mark.parametrize("code", sorted(FIRES))
def test_each_membership_finding_is_reported_for_the_input_it_describes(code: str) -> None:
    assert coded(FIRES[code]().diagnostics, code)


def test_each_invariant_fires_on_an_attachment_that_breaks_it() -> None:
    metric = member("metric", "m", ("products",))
    query = member("verified_query", "q", ("orders",))
    built = request(metric, query)
    first = {metric.key: ("semantic_view:menu",), query.key: ("semantic_view:menu", "semantic_view:sales")}
    placed = {metric.key: (), query.key: ("semantic_view:menu",)}
    found = invariant_diagnostics(built, first, placed, {**placed, metric.key: ("semantic_view:menu",)})
    assert _codes(found) == ["SST-MEM013", "SST-MEM900", "SST-MEM106"]


def test_a_view_outside_the_reported_set_holds_nothing_reported() -> None:
    result = membership(member("metric", "m", ("orders",)), reported_views=frozenset(("semantic_view:menu",)))
    assert [item.subject for item in coded(result.diagnostics, "SST-MEM103")] == ["semantic_view:menu"]
