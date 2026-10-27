"""SST-VAL204: a condition column is on a table other than its side's endpoint."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.val_codes import parsed_conditions

KEY = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"


def test_sst_val204_fires() -> None:
    found = parsed_conditions("{{ ref('orders', 'customer_id') }} = {{ ref('people', 'customer_id') }}")
    assert isinstance(found, Diagnostic) and found.code == "SST-VAL204"
    assert found.severity is Severity.ERROR
    assert found.message == "relationship 'rel': 'people.customer_id' is not on 'customers'"
    assert found.subject == "relationship:rel"


def test_sst_val204_silent() -> None:
    assert isinstance(parsed_conditions(KEY), _Conditions)
