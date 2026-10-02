"""The checks domain runs over built semantic views, and the rules every artifact type shares."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
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
from snowflake_semantic_tools.domain.plan.classify import classify
from snowflake_semantic_tools.domain.plan.prune import block_unpublished_dependencies
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.validate.semantic_view import (
    fan_out_diagnostics,
    join_graph_diagnostics,
    relation_diagnostics,
    restriction_diagnostics,
    statement_diagnostics,
)
from snowflake_semantic_tools.domain.validate.shared import (
    is_digest,
    lacks_invocation,
    namespace_collisions,
    reference_cycle,
    unmodelled_key_diagnostics,
)
from tests.helpers.artifact_builders import change, marker, observed, rendered
from tests.helpers.sql_values import authored, authored_query

TABLES = tuple(Table(name, f"DB.S.{name}") for name in ("ORDERS", "CUSTOMERS", "LOCATIONS", "PERIODS"))


def _join(name: str, left: str, right: str, **changes: object) -> Relationship:
    return Relationship(name, left, ("K",), right, ("K",), **changes)  # type: ignore[arg-type]


def test_a_statement_is_checked_for_grant_loss_and_tags_on_create_or_alter() -> None:
    assert [item.code for item in statement_diagnostics("CREATE OR REPLACE SEMANTIC VIEW V", artifact="v")] == [
        "SST-VAL307"
    ]
    assert [item.code for item in statement_diagnostics("CREATE OR ALTER X\n  WITH TAG (", artifact="v")] == [
        "SST-VAL321"
    ]
    assert statement_diagnostics("CREATE OR ALTER X", artifact="v") == ()
    assert statement_diagnostics("CREATE OR REPLACE X\n  COPY GRANTS", artifact="v") == ()


def test_only_an_error_of_the_critical_set_names_a_metric_as_unchecked() -> None:
    view = SemanticView("DB.S.V", TABLES, metrics=(Metric("B", authored("1")), Metric("A", authored("1"))))
    reported = (
        D("SST-VAL103", metric="a", other="x", subject="metric:a"),
        D("SST-VAL103", metric="b", other="x"),
        D("SST-VAL124", metric="b", other="a", subject="metric:b"),
    )
    assert [item.context["metric"] for item in restriction_diagnostics(view, reported, artifact="v")] == ["a"]


def test_a_table_is_checked_against_its_model_relation_when_one_is_known() -> None:
    tables = (Table("ORDERS", "DB.S.ORDERS"), Table("PERIODS", "DB.S.PERIODS"), Table("OTHER", "DB.S.OTHER"))
    found = relation_diagnostics(tables, {"ORDERS": "db.s.orders", "PERIODS": "DB.S.CALENDAR"}, artifact="v")
    assert [item.context["name"] for item in found] == ["periods"]


def test_the_join_graph_reports_its_shape_and_each_bridge_pair_once() -> None:
    relationships = (
        _join("O_TO_C", "ORDERS", "CUSTOMERS"),
        _join("O_TO_C_AGAIN", "ORDERS", "CUSTOMERS"),
        _join("O_TO_L", "ORDERS", "LOCATIONS"),
        _join("O_TO_P", "ORDERS", "PERIODS", range_bounds=("S", "E")),
        _join("C_TO_L", "CUSTOMERS", "LOCATIONS", asof_index=0),
        _join("HIDDEN", "LOCATIONS", "PERIODS"),
    )
    view = SemanticView(
        "DB.S.V", TABLES, relationships=relationships, scope=ViewScope(exclude_relationships=("HIDDEN",))
    )
    found = join_graph_diagnostics(view, artifact="v")
    assert [(item.code, item.context["value"]) for item in found] == [
        ("SST-VAL217", "5 relationships in 1 connected part"),
        ("SST-VAL216", "CUSTOMERS <-> LOCATIONS"),
    ]
    excluded = SemanticView("DB.S.V", TABLES, relationships=relationships[-1:], scope=view.scope)
    assert join_graph_diagnostics(excluded, artifact="v") == ()


def test_fan_out_counts_what_each_view_holds_and_the_metrics_in_several() -> None:
    query = VerifiedQuery("Q", "How many?", authored_query("SELECT 1"))
    full = SemanticView(
        "DB.S.FULL",
        TABLES,
        columns=(Column("ORDERS", "IS_DONE", ColumnKind.FILTER, authored("TRUE")),),
        metrics=(Metric("M", authored("COUNT(1)"), "ORDERS"), Metric("HIDDEN", authored("COUNT(1)"), "ORDERS")),
        verified_queries=(query, query),
        scope=ViewScope(exclude_metrics=("HIDDEN",)),
    )
    other = SemanticView("DB.S.OTHER", TABLES, metrics=(Metric("M", authored("COUNT(1)"), "ORDERS"),))
    found = fan_out_diagnostics(((full, "full"), (other, "other"), (SemanticView("DB.S.E", TABLES), "empty")))
    assert [(item.code, item.subject, item.message) for item in found] == [
        ("SST-VAL319", "full", "full: 1 filter, 1 metric, 2 verified queries by table membership"),
        ("SST-VAL319", "other", "other: 1 metric by table membership"),
        ("SST-VAL125", "metric:m", "metric 'm' attaches to 2 views"),
    ]


def test_a_description_states_when_to_invoke_or_is_not_judged() -> None:
    assert lacks_invocation("Customers and their orders.")
    assert not lacks_invocation("Use for questions about customers.")
    assert not lacks_invocation(None) and not lacks_invocation("  ")


def test_the_first_reference_cycle_is_found_in_a_stable_order() -> None:
    assert reference_cycle({"b": ("c",), "a": ("b", "outside"), "c": ("b",)}) == ("b", "c", "b")
    assert reference_cycle({"a": ("b",), "b": (), "c": ("a",)}) == ()


def test_a_name_another_namespace_holds_collides() -> None:
    found = namespace_collisions((("skill:a", "skill", "A"), ("skill:b", "skill", "b")), {"a": "extension 'X'"})
    assert [(item.code, item.subject, item.context["other"]) for item in found] == [
        ("SST-VAL002", "skill:a", "extension 'X'")
    ]


def test_unmodelled_keys_warn_once_or_error_per_key() -> None:
    warned = unmodelled_key_diagnostics("agent", "a", ("y", "x", "y"), allow=True)
    assert [(item.code, item.severity, item.context["count"]) for item in warned] == [
        ("SST-VAL014", Severity.WARNING, 2)
    ]
    refused = unmodelled_key_diagnostics("agent", "a", ("y", "x"), allow=False)
    assert [(item.code, item.context["key"]) for item in refused] == [("SST-VAL013", "x"), ("SST-VAL013", "y")]
    assert unmodelled_key_diagnostics("agent", "a", (), allow=False) == ()


def test_only_a_lowercase_sha256_is_a_digest() -> None:
    assert is_digest("a" * 64)
    assert not is_digest("A" * 64) and not is_digest("CREATE OR REPLACE")


def test_only_a_write_whose_compiled_dependency_is_absent_and_unplanned_is_blocked() -> None:
    dependent = change(rendered("A", depends_on=("semantic_view:b", "semantic_view:c")))
    observed_dependency = change(rendered("D", depends_on=("semantic_view:e",)))
    pruned = change(rendered("F", depends_on=("semantic_view:b",)), Action.PRUNE)
    changes, found = block_unpublished_dependencies(
        (dependent, observed_dependency, pruned),
        {"semantic_view:e"},
        {"semantic_view:b", "semantic_view:e"},
        ARTIFACT_REGISTRY,
    )
    assert [(item.key, item.action) for item in changes] == [
        ("semantic_view:a", Action.BLOCKED),
        ("semantic_view:d", Action.CREATE),
        ("semantic_view:f", Action.PRUNE),
    ]
    assert [item.context["blocker"] for item in found] == ["semantic_view:b"]


def test_plan_blocks_a_comparison_against_a_recorded_value_that_is_not_a_digest() -> None:
    artifact = rendered("V")
    recorded = AppliedEntry("raw ddl", artifact.target.sql, "now", "run", "applied", "raw ddl", "m" * 64)
    decision = classify(
        artifact.key,
        artifact,
        ARTIFACT_REGISTRY,
        observed=observed(artifact, ownership=marker(artifact)),
        recorded=recorded,
        manifest_id="m" * 64,
        validation=DiagnosticBag(),
        composite=None,
    )
    assert decision.change.action is Action.BLOCKED
    assert [item.code for item in decision.diagnostics] == ["SST-VAL320"]
