"""SST-VAL008: a structural field (SQL, an expression, an instruction) uses a folded scalar."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import FILTERS, VIEWS, edited, reported

EXPR = "    expr: \"{{ ref('orders', 'order_state') }} = '{{ var('completed_state') }}'\"\n"
FOLDED_EXPR = "    expr: >-\n      {{ ref('orders', 'order_state') }} = '{{ var('completed_state') }}'\n"
LITERAL = "  - name: jaffle_sales\n    description: |-\n"


def test_sst_val008_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, FILTERS, EXPR, FOLDED_EXPR)
    [diagnostic] = reported(project, "SST-VAL008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter 'is_completed_order': 'expr' uses a folded scalar"
    assert diagnostic.subject == "filter:is_completed_order"


def test_sst_val008_silent(tmp_path: Path) -> None:
    # A description may fold: `sst format` writes a long line that way, and it loads as one line.
    folded = edited(tmp_path, VIEWS, LITERAL, "  - name: jaffle_sales\n    description: >-\n")
    assert reported(folded, "SST-VAL008") == []
