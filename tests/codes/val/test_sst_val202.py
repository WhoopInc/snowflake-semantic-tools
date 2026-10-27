"""SST-VAL202: a parsed condition would not survive rendering into the DDL."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Diagnostic, Severity
from tests.helpers.val_codes import parsed_conditions

KEY = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"


FIRST_ASOF = "{{ ref('orders', 'ordered_at') }} >= {{ ref('customers', 'first_at') }}"
LAST_ASOF = "{{ ref('orders', 'placed_at') }} >= {{ ref('customers', 'joined_at') }}"
RANGE = (
    "{{ ref('orders', 'ordered_at') }} BETWEEN {{ ref('customers', 'start_at') }} AND {{ ref('customers', 'end_at') }}"
)
SECOND_RANGE = (
    "{{ ref('orders', 'placed_at') }} BETWEEN {{ ref('customers', 'start_at') }} AND {{ ref('customers', 'end_at') }}"
)


def test_sst_val202_fires() -> None:
    # A second ASOF is SST-PRS112's; a second range is dropped as silently.
    found = parsed_conditions(RANGE, SECOND_RANGE)
    assert isinstance(found, Diagnostic) and found.code == "SST-VAL202"
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == f"relationship 'rel': condition '{RANGE}' would be dropped from the DDL"
    assert found.subject == "relationship:rel"
    assert parsed_conditions(KEY, FIRST_ASOF, LAST_ASOF).code == "SST-PRS112"  # type: ignore[union-attr]
    # A range renders alone, so the equality beside it would be lost.
    beside = parsed_conditions(KEY, RANGE)
    assert isinstance(beside, Diagnostic) and beside.context["value"] == KEY


def test_sst_val202_silent() -> None:
    assert isinstance(parsed_conditions(KEY, LAST_ASOF), _Conditions)
    assert isinstance(parsed_conditions(RANGE), _Conditions)
