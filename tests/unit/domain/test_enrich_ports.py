"""The ports `sst enrich` reads Snowflake through."""

from __future__ import annotations

from snowflake_semantic_tools.domain.ports.enrich import CortexPort, EnrichPort, RelationProfilerPort


def test_the_enrich_port_is_the_profiler_and_cortex_roles_together() -> None:
    assert EnrichPort.__mro__[1:3] == (RelationProfilerPort, CortexPort)
    assert {"relation_columns", "distinct_values", "complete_json"} <= set(dir(EnrichPort))
