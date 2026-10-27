"""SST-VAL302: a table would render against something other than its model's resolved relation."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Table
from snowflake_semantic_tools.domain.validate.semantic_view import relation_diagnostics

# pricing_periods is aliased: dbt builds it as PRICING_CALENDAR.
RELATIONS = {"PRICING_PERIODS": "DB.S.PRICING_CALENDAR"}


def test_sst_val302_fires() -> None:
    by_model_name = (Table("PRICING_PERIODS", "DB.S.PRICING_PERIODS"),)
    [found] = relation_diagnostics(by_model_name, RELATIONS, artifact="semantic_view:menu")
    assert found.severity is Severity.ERROR
    assert found.message == "semantic_view:menu: table 'pricing_periods' resolves to relation 'DB.S.PRICING_CALENDAR'"
    assert found.subject == "semantic_view:menu"


def test_sst_val302_silent() -> None:
    by_relation = (Table("PRICING_PERIODS", "db.s.pricing_calendar"),)
    assert relation_diagnostics(by_relation, RELATIONS, artifact="semantic_view:menu") == ()
