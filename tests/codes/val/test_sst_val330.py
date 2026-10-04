"""SST-VAL330: a view's scope names an item its tables do not provide."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import reported, view_names, with_view

VIEW = """  - name: v
    description: |-
      Use this view for questions about orders.
    tables:
      - "{{{{ ref('customers') }}}}"
    {key}:
      - "{entry}"
"""


@pytest.mark.parametrize(
    ("key", "entry", "message"),
    [
        ("metrics", "{{ metric('no_such_metric') }}", "metrics names metric 'no_such_metric', which does not exist"),
        (
            "exclude_metrics",
            "{{ metric('order_count') }}",
            "exclude_metrics names metric 'order_count', which does not attach to this view",
        ),
        (
            "columns",
            "{{ ref('orders', 'order_id') }}",
            "columns names column 'orders.order_id', which is not on a table of this view",
        ),
        (
            "columns",
            "{{ ref('customers', 'no_such_column') }}",
            "columns names column 'customers.no_such_column', which does not exist on 'customers'",
        ),
        ("columns", "customer_id", "columns names column 'customer_id', which is not a two-argument ref() call"),
        (
            "exclude_relationships",
            "no_such_relationship",
            "exclude_relationships names relationship 'no_such_relationship', which does not exist",
        ),
    ],
)
def test_sst_val330_fires(tmp_path: Path, key: str, entry: str, message: str) -> None:
    project = with_view(tmp_path, VIEW.format(key=key, entry=entry))
    [diagnostic] = reported(project, "SST-VAL330")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"semantic_view:v: {message}"
    assert diagnostic.subject == "semantic_view:v"
    assert "V" not in view_names(project)


def test_sst_val330_silent(tmp_path: Path) -> None:
    project = with_view(tmp_path, VIEW.format(key="exclude_columns", entry="{{ ref('customers', 'customer_name') }}"))
    assert reported(project, "SST-VAL330") == []
    assert "V" in view_names(project)
