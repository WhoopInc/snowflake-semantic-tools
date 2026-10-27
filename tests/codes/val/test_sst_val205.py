"""SST-VAL205: a relationship joins two tables no view holds together."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _relationship_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship

JOIN = Relationship("CUSTOMERS_TO_LOCATIONS", "CUSTOMERS", ("LOCATION_ID",), "LOCATIONS", ("LOCATION_ID",))


def test_sst_val205_fires() -> None:
    found = _relationship_diagnostics((JOIN,), (("semantic_view:sales", frozenset(("customers", "orders"))),))
    assert found[0].code == "SST-VAL205" and found[0].severity is Severity.ERROR
    assert (
        found[0].message
        == "relationship 'customers_to_locations' joins 'customers' and 'locations', which share no view"
    )
    assert found[0].subject == "relationship:customers_to_locations"


def test_sst_val205_silent() -> None:
    found = _relationship_diagnostics((JOIN,), (("semantic_view:geo", frozenset(("customers", "locations"))),))
    assert [item.code for item in found if item.code == "SST-VAL205"] == []
