"""SST-VAL213: one side of a condition spans more than one column."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.val_codes import parsed_conditions

KEY = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"


SPANNING = "{{ ref('orders', 'first') }} || {{ ref('orders', 'last') }} = {{ ref('customers', 'full_name') }}"


def test_sst_val213_fires() -> None:
    found = parsed_conditions(SPANNING)
    assert isinstance(found, Diagnostic) and found.code == "SST-VAL213"
    assert found.severity is Severity.ERROR
    assert found.message == f"relationship 'rel': condition '{SPANNING}' spans multiple columns per side"
    assert found.subject == "relationship:rel"


def test_sst_val213_silent() -> None:
    assert isinstance(parsed_conditions(KEY), _Conditions)
    # Some other shape is still a parse error, not a multi-column one.
    other = parsed_conditions("{{ ref('orders', 'customer_id') }} == {{ ref('customers', 'customer_id') }}")
    assert isinstance(other, Diagnostic) and other.code == "SST-PRS110"
