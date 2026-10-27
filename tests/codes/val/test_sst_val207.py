"""SST-VAL207: a relationship joins one logical table to itself."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _relationship_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship

VIEWS = (("semantic_view:geo", frozenset(("locations", "regions"))),)


def test_sst_val207_fires() -> None:
    itself = Relationship("LOCATIONS_TO_LOCATIONS", "LOCATIONS", ("PARENT_ID",), "LOCATIONS", ("LOCATION_ID",))
    [found] = _relationship_diagnostics((itself,), VIEWS)
    assert found.severity is Severity.ERROR
    assert found.message == "relationship 'locations_to_locations' joins 'locations' to itself"
    assert found.subject == "relationship:locations_to_locations"


def test_sst_val207_silent() -> None:
    across = Relationship("LOCATIONS_TO_REGIONS", "LOCATIONS", ("REGION_ID",), "REGIONS", ("REGION_ID",))
    assert [item.code for item in _relationship_diagnostics((across,), VIEWS) if item.code == "SST-VAL207"] == []
