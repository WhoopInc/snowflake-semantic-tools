"""SST-REF045: a relationship endpoint is written as a `{{ ref() }}` call instead of the model name."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, edited_fixture

FILE = "semantic_models/relationships/relationships.yml"
BEFORE = "    left_table: orders"


def test_sst_ref045_fires(tmp_path: Path) -> None:
    project = edited_fixture(tmp_path, FILE, BEFORE, "    left_table: \"{{ ref('orders') }}\"")
    [diagnostic] = coded(project.diagnostics, "SST-REF045")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "relationship:orders_to_customers: left_table is written as {{ ref('orders') }}; "
        "it takes the bare model name"
    )
    assert diagnostic.subject == "relationship:orders_to_customers"


def test_sst_ref045_silent(tmp_path: Path) -> None:
    project = edited_fixture(tmp_path, FILE, BEFORE, '    left_table: "orders"')
    assert coded(project.diagnostics, "SST-REF045") == []
