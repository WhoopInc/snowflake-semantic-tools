"""SST-VAL213: one side of a condition spans more than one column."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions, _parse_conditions
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity

ORIGIN = Origin("relationships.yml", 1, 1)
ENDPOINTS = ("orders", "customers")
KEY = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"


def _parse(*conditions: str) -> _Conditions | Diagnostic:
    return _parse_conditions(list(conditions), ENDPOINTS, "rel", ORIGIN, "relationship:rel")


SPANNING = "{{ ref('orders', 'first') }} || {{ ref('orders', 'last') }} = {{ ref('customers', 'full_name') }}"


def test_sst_val213_fires() -> None:
    found = _parse(SPANNING)
    assert isinstance(found, Diagnostic) and found.code == "SST-VAL213"
    assert found.severity is Severity.ERROR
    assert found.message == f"relationship 'rel': condition '{SPANNING}' spans multiple columns per side"
    assert found.subject == "relationship:rel"


def test_sst_val213_silent() -> None:
    assert isinstance(_parse(KEY), _Conditions)
    # Some other shape is still a parse error, not a multi-column one.
    other = _parse("{{ ref('orders', 'customer_id') }} == {{ ref('customers', 'customer_id') }}")
    assert isinstance(other, Diagnostic) and other.code == "SST-PRS110"
