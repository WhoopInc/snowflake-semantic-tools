"""The orders the semantic checks and the view build run their rules in, which decide what is reported."""

from __future__ import annotations

import shutil
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_column_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.metrics import _metric_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions, _parse_conditions
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import Diagnostic, Origin
from tests.helpers.projects import load_project

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"

ORDERS = DbtModel(
    "model.fixture.orders",
    "orders",
    "DB.SCH.ORDERS",
    ("order_id",),
    (),
    (DbtColumn("order_id", "Key.", "VARCHAR", "dimension"), DbtColumn("amount", "Amount.", "NUMBER", "fact")),
)


def _codes(diagnostics: tuple[Diagnostic, ...]) -> list[str]:
    return [diagnostic.code for diagnostic in diagnostics]


def test_a_malformed_template_ends_its_metrics_checks_before_the_bare_identifier_rule() -> None:
    broken = MetricDef(
        "broken",
        "SUM({{ ref('orders', 'amount' }} + amount)",
        "Broken.",
        (),
        ("orders",),
        access_modifier="hidden",
        has_tables_key=True,
    )
    bare = MetricDef("bare", "SUM(amount)", "Bare.", (), ("orders",), has_tables_key=True)
    diagnostics = _metric_diagnostics((broken, bare), {"orders": ORDERS})
    # `broken` names `amount` bare too, but its template does not scan, so only `bare` is reported.
    assert _codes(diagnostics) == ["SST-VAL121", "SST-LOD004", "SST-VAL110"]
    assert diagnostics[2].subject == "metric:bare"


def test_a_repeated_metric_name_is_checked_no_further_but_still_counts_for_equivalence() -> None:
    expression = "SUM({{ ref('orders', 'amount') }})"
    metrics = (
        MetricDef("dup", expression, "First.", (), ("orders",), has_tables_key=True),
        MetricDef("DUP", expression, "Second.", (), ("orders",), has_tables_key=True),
        MetricDef("later", "sum({{ ref('orders',  'amount') }})", "Later.", (), ("orders",), has_tables_key=True),
    )
    diagnostics = _metric_diagnostics(metrics, {"orders": ORDERS})
    assert _codes(diagnostics) == ["SST-VAL001", "SST-VAL124"]
    # The second spelling of the repeated name takes the first's place as the metric compared against.
    assert dict(diagnostics[1].context) == {"metric": "later", "other": "DUP"}


def test_column_rules_run_in_order_and_only_sentinels_are_reported_on_an_unused_model() -> None:
    column = DbtColumn(
        "c",
        None,
        None,
        None,
        sample_values=("a", "b", "c", "d", "null"),
        unknown_meta_keys=("zeta",),
        declared_data_type="NUMBER",
    )
    used = DbtModel("model.fixture.used", "used", "DB.SCH.USED", ("c",), (), (column,))
    unused = DbtModel("model.fixture.unused", "unused", "DB.SCH.UNUSED", ("c",), (), (column,))
    diagnostics = _dbt_column_diagnostics({"used": used, "unused": unused}, frozenset({"used"}))
    assert [(diagnostic.code, diagnostic.subject) for diagnostic in diagnostics] == [
        ("SST-VAL316", "dbt_column:unused.c"),
        ("SST-VAL308", "dbt_column:used.c"),
        ("SST-VAL309", "dbt_column:used.c"),
        ("SST-VAL315", "dbt_column:used.c"),
        ("SST-VAL003", "dbt_column:used.c"),
        ("SST-PRS004", "dbt_column:used.c"),
        ("SST-DBT004", "dbt_column:used.c"),
        ("SST-VAL316", "dbt_column:used.c"),
    ]


def test_a_relationship_reports_the_first_bad_condition_and_keeps_the_last_asof_column() -> None:
    origin = Origin("relationships.yml", 1, 1)
    endpoints = ("orders", "customers")
    misplaced = "{{ ref('orders', 'customer_id') }} = {{ ref('people', 'customer_id') }}"
    malformed = "{{ ref('orders', 'customer_id') }} == {{ ref('customers', 'customer_id') }}"
    first = _parse_conditions([misplaced, malformed], endpoints, "rel", origin, "relationship:rel")
    second = _parse_conditions([malformed, misplaced], endpoints, "rel", origin, "relationship:rel")
    assert isinstance(first, Diagnostic) and first.code == "SST-VAL204"
    assert isinstance(second, Diagnostic) and second.code == "SST-PRS110"
    parsed = _parse_conditions(
        [
            "{{ ref('orders', 'ordered_at') }} >= {{ ref('customers', 'first_at') }}",
            "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}",
            "{{ ref('orders', 'placed_at') }} >= {{ ref('customers', 'joined_at') }}",
        ],
        endpoints,
        "rel",
        origin,
        "relationship:rel",
    )
    assert parsed == _Conditions(
        (("ORDERED_AT", "FIRST_AT"), ("CUSTOMER_ID", "CUSTOMER_ID"), ("PLACED_AT", "JOINED_AT")), 2, None
    )


def test_a_view_reports_its_verified_query_failure_before_a_bad_variable(tmp_path: Path) -> None:
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project, ignore=shutil.ignore_patterns("target", "logs"))
    (project / "semantic_models" / "verified_queries" / "extra.yml").write_text(
        "snowflake_verified_queries:\n"
        "  - name: products_per_order\n"
        "    tables: [orders]\n"
        '    question: "Products per order?"\n'
        "    sql: \"SELECT {{ metric('product_count') }} FROM orders\"\n",
        encoding="utf-8",
    )
    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    variable = "      - name: tax_inclusive\n        data_type: BOOLEAN\n"
    text = views.read_text(encoding="utf-8")
    assert variable in text
    bad_variable = "      - name: bad_bool\n        data_type: BOOLEAN\n        default_value: 3\n"
    views.write_text(text.replace(variable, bad_variable + variable, 1), encoding="utf-8")
    result = load_project(project, manifest_path=MANIFEST)
    # jaffle_sales holds `orders` but not `products`, so the query's metric() does not resolve
    # there; the build stops at that phase and never reaches the variables.
    sales = [diagnostic for diagnostic in result.diagnostics if diagnostic.subject == "semantic_view:jaffle_sales"]
    assert [(diagnostic.code, dict(diagnostic.context)) for diagnostic in sales] == [
        ("SST-REF006", {"name": "product_count"})
    ]
    assert sorted(view.fqn for view in result.views) == [
        "SST_REF_DEV.CORE.JAFFLE_MENU",
        "SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL",
    ]
