"""SST-VAL202: a parsed condition would not survive rendering into the DDL."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions, _parse_conditions
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Diagnostic, Origin, Severity

ORIGIN = Origin("relationships.yml", 1, 1)
ENDPOINTS = ("orders", "customers")
KEY = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"


def _parse(*conditions: str) -> _Conditions | Diagnostic:
    return _parse_conditions(list(conditions), ENDPOINTS, "rel", ORIGIN, "relationship:rel")


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
    found = _parse(RANGE, SECOND_RANGE)
    assert isinstance(found, Diagnostic) and found.code == "SST-VAL202"
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == f"relationship 'rel': condition '{RANGE}' would be dropped from the DDL"
    assert found.subject == "relationship:rel"
    assert _parse(KEY, FIRST_ASOF, LAST_ASOF).code == "SST-PRS112"  # type: ignore[union-attr]
    # A range renders alone, so the equality beside it would be lost.
    beside = _parse(KEY, RANGE)
    assert isinstance(beside, Diagnostic) and beside.context["value"] == KEY


def test_sst_val202_silent() -> None:
    assert isinstance(_parse(KEY, LAST_ASOF), _Conditions)
    assert isinstance(_parse(RANGE), _Conditions)
