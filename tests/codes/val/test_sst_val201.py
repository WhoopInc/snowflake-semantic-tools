"""SST-VAL201: a relationship declares no conditions."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import RELATIONSHIPS, edited, reported

CONDITIONS = (
    "    relationship_conditions:\n"
    "      - \"{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}\"\n"
)


def test_sst_val201_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(
        edited(tmp_path, RELATIONSHIPS, CONDITIONS, "    relationship_conditions: []\n"), "SST-VAL201"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "relationship 'orders_to_customers' declares no conditions"
    assert diagnostic.subject == "relationship:orders_to_customers"


def test_sst_val201_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, RELATIONSHIPS, CONDITIONS, CONDITIONS), "SST-VAL201") == []
