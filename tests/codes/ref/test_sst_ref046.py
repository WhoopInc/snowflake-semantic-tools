"""SST-REF046: a relationship endpoint is not one `{{ ref('<model>') }}` call."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import RELATIONSHIPS, edited, load

BEFORE = "    left_table: \"{{ ref('orders') }}\"\n    right_table: \"{{ ref('customers') }}\"\n"


def test_sst_ref046_fires(tmp_path: Path) -> None:
    after = "    left_table: orders\n    right_table: \"{{ ref('customers') }}\"\n"
    [diagnostic] = coded(load(edited(tmp_path, RELATIONSHIPS, BEFORE, after)).diagnostics, "SST-REF046")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "relationship:orders_to_customers: left_table is 'orders'; it takes one {{ ref('<model>') }} call"
    )
    assert diagnostic.subject == "relationship:orders_to_customers"


@pytest.mark.parametrize(
    "endpoint",
    ["\"{{ table('orders') }}\"", "\"{{ ref('orders', 'order_id') }}\"", "\"{{ ref('orders') }} x\""],
)
def test_sst_ref046_fires_for_anything_but_one_ref_call(tmp_path: Path, endpoint: str) -> None:
    after = f"    left_table: {endpoint}\n    right_table: \"{{{{ ref('customers') }}}}\"\n"
    assert len(coded(load(edited(tmp_path, RELATIONSHIPS, BEFORE, after)).diagnostics, "SST-REF046")) == 1


def test_sst_ref046_silent(tmp_path: Path) -> None:
    after = "    left_table: \"{{ ref( 'orders' ) }}\"\n    right_table: \"{{ ref('customers') }}\"\n"
    assert coded(load(edited(tmp_path, RELATIONSHIPS, BEFORE, after)).diagnostics, "SST-REF046") == []
