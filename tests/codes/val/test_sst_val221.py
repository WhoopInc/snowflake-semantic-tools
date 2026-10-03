"""SST-VAL221: a filter that renders as prose holds a bare word that is neither a column nor a variable."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import FILTERS, edited, found

PROSE = '    expr: "large_order_cents"\n'


def test_sst_val221_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, FILTERS, PROSE, '    expr: "large_ordr_cents"\n')
    diagnostic = next(item for item in found(project, "SST-VAL221") if item.subject == "semantic_view:jaffle_sales")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:jaffle_sales: 'large_ordr_cents' in the expr of 'high_value_threshold_cents' "
        "is neither a column nor a declared variable"
    )
    # The prose fails no create, so the word is not also an error.
    assert found(project, "SST-VAL326") == []


def test_sst_val221_silent(tmp_path: Path) -> None:
    project = edited(tmp_path, FILTERS, PROSE, PROSE)
    assert found(project, "SST-VAL221") == []
    # A typo in an expression the view is created with is SST-VAL326's, never this warning.
    typo = edited(
        tmp_path / "typo",
        FILTERS,
        "{{ ref('orders', 'order_total') }} > large_order_cents",
        "{{ ref('orders', 'order_total') }} > large_ordr_cents",
    )
    assert found(typo, "SST-VAL221") == []
