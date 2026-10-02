"""SST-MEM009: a metric claims a synonym that something else in its view already claims.

A table and its columns may share a synonym -- a foreign key and the key it references
describe one entity, and the reference project does exactly that -- so only a clash that
involves a metric is reported.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, edited_fixture, reference_fixture

METRICS = "semantic_models/metrics/metrics.yml"
REVENUE_SYNONYMS = "      - revenue\n      - gross sales"


def test_sst_mem009_fires(tmp_path: Path) -> None:
    # `menu item` is already the products table's synonym in jaffle_menu.
    project = edited_fixture(tmp_path, METRICS, REVENUE_SYNONYMS, "      - menu item\n      - gross sales")
    [diagnostic] = coded(project.diagnostics, "SST-MEM009")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:jaffle_menu: synonym 'menu item' is claimed by PRODUCTS and ORDERS.TOTAL_REVENUE"
    )
    assert diagnostic.subject == "semantic_view:jaffle_menu"


def test_sst_mem009_silent(tmp_path: Path) -> None:
    assert coded(reference_fixture(tmp_path).diagnostics, "SST-MEM009") == []
