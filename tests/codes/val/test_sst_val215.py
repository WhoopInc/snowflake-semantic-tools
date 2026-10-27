"""SST-VAL215: a view's relationships form a cycle between two or more tables."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _relationship_cycle_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship

TO_REGIONS = Relationship("LOCATIONS_TO_REGIONS", "LOCATIONS", ("REGION_ID",), "REGIONS", ("REGION_ID",))
BACK = Relationship("REGIONS_TO_LOCATIONS", "REGIONS", ("HQ_ID",), "LOCATIONS", ("LOCATION_ID",))
VIEWS = (("semantic_view:loop", frozenset(("locations", "regions"))),)


def test_sst_val215_fires() -> None:
    [found] = _relationship_cycle_diagnostics((TO_REGIONS, BACK), VIEWS)
    assert found.severity is Severity.ERROR
    assert found.message == "semantic_view:loop: relationship cycle locations -> regions -> locations"
    assert found.subject == "semantic_view:loop"


def test_sst_val215_silent() -> None:
    assert _relationship_cycle_diagnostics((TO_REGIONS,), VIEWS) == ()
