"""SST-VAL211: a relationship declares relationship_type or join_type, which is not emitted."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import RELATIONSHIPS, edited, found

LEFT = "    left_table: orders\n    right_table: customers\n"


def test_sst_val211_fires(tmp_path: Path) -> None:
    [diagnostic] = found(edited(tmp_path, RELATIONSHIPS, LEFT, LEFT + "    join_type: left_outer\n"), "SST-VAL211")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "relationship 'orders_to_customers' declares 'join_type', which is not emitted"
    assert diagnostic.subject == "relationship:orders_to_customers"


def test_sst_val211_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, RELATIONSHIPS, LEFT, LEFT), "SST-VAL211") == []
