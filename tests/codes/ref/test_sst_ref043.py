"""SST-REF043: a filter's expression refs a model outside its declared `tables:`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, edited_fixture

FILE = "semantic_models/filters/filters.yml"
BEFORE = "    expr: \"{{ ref('orders', 'order_total') }} > large_order_cents\""


def test_sst_ref043_fires(tmp_path: Path) -> None:
    project = edited_fixture(
        tmp_path,
        FILE,
        BEFORE,
        "    expr: \"{{ ref('orders', 'order_total') }} > large_order_cents AND {{ ref('customers') }} IS NOT NULL\"",
    )
    [diagnostic] = coded(project.diagnostics, "SST-REF043")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "filter:is_large_order: ref('customers') is not one of the view's tables"
    assert diagnostic.subject == "filter:is_large_order"


def test_sst_ref043_silent(tmp_path: Path) -> None:
    project = edited_fixture(
        tmp_path,
        FILE,
        BEFORE,
        "    expr: \"{{ ref('orders', 'order_total') }} > large_order_cents AND {{ ref('orders') }} IS NOT NULL\"",
    )
    assert coded(project.diagnostics, "SST-REF043") == []
