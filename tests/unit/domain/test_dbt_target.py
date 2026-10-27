"""A resolved dbt target qualifies an object name without touching its own database or schema."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.dbt import DbtTarget


def test_a_target_upper_cases_only_the_name_it_qualifies() -> None:
    assert DbtTarget(database="SST_REF_DEV", schema="Jaffle").fqn("jaffle_sales") == "SST_REF_DEV.Jaffle.JAFFLE_SALES"
