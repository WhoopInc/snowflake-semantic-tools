"""SST-VAL326: an expression a view is created with holds a bare name the view cannot resolve."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import FILTERS, METRICS, edited, found, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about orders.
    tables:
      - "{{ ref('orders') }}"
    variables:
      - name: tax_inclusive
        data_type: BOOLEAN
        default_value: false
"""
LARGE = """      - name: large_order_cents
        data_type: NUMBER
        default_value: 1000
"""
EXPR = "    expr: \"SUM({{ ref('orders', 'order_total') }})\"\n"


def test_sst_val326_fires(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW)
    diagnostic = next(item for item in found(project, "SST-VAL326") if item.context["member"] == "large_order_count")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "view 'v': member 'large_order_count' references 'large_order_cents', which is neither a column on the "
        "view's tables nor a variable the view declares"
    )
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val326_fires_on_a_typo(tmp_path: Path) -> None:
    project = edited(tmp_path, METRICS, EXPR, "    expr: \"SUM({{ ref('orders', 'order_total') }}) * fudge_factor\"\n")
    diagnostic = next(item for item in found(project, "SST-VAL326") if item.subject == "semantic_view:jaffle_sales")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "view 'jaffle_sales': member 'total_revenue' references 'fudge_factor', which is neither a column on the "
        "view's tables nor a variable the view declares"
    )
    assert found(project, "SST-VAL221") == []
    assert "JAFFLE_SALES" not in view_names(project)


def test_sst_val326_silent(tmp_path: Path) -> None:
    assert found(with_view(tmp_path, VIEW + LARGE), "SST-VAL326") == []
    # A typo in a filter that renders as prose fails no create: SST-VAL221's warning, not this.
    prose = edited(tmp_path / "prose", FILTERS, '    expr: "large_order_cents"\n', '    expr: "large_ordr_cents"\n')
    assert found(prose, "SST-VAL326") == []
    # Niladic SQL functions are SQL, not names.
    niladic = edited(
        tmp_path / "niladic",
        METRICS,
        EXPR,
        "    expr: \"SUM({{ ref('orders', 'order_total') }}) + 0 * DATEDIFF(day, CURRENT_DATE, CURRENT_DATE)\"\n",
    )
    assert found(niladic, "SST-VAL326") == []
