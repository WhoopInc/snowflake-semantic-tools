"""SST-VAL328: a sensitive column carries sample values."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings, enrich_findings

EMAIL = (WarehouseColumn("EMAIL", "TEXT"),)


def test_sst_val328_fires() -> None:
    tagged = DbtColumn("email", None, "VARCHAR", "dimension", sample_values=("a@b.c",), pii_tagged=True)
    [found] = enrich_findings(tagged, EMAIL, {"email": ["x@y.z"]}, "SST-VAL328", Component.SAMPLE_VALUES)
    assert found.severity is Severity.WARNING
    assert found.message == "model 'orders': column 'email' carries pii_tags and 1 sample_values"
    assert found.subject == "dbt_column:orders.email"


def test_sst_val328_silent() -> None:
    untagged = DbtColumn("email", None, "VARCHAR", "dimension", pii_tagged=True)
    assert enrich_findings(untagged, EMAIL, {"email": ["x@y.z"]}, "SST-VAL328", Component.SAMPLE_VALUES) == []


def test_sst_val328_fires_offline_and_in_enrich_for_a_governance_tag() -> None:
    recorded = DbtColumn("email", "e", "VARCHAR", "dimension", sample_values=("a", "b"), governance_tags=("A.B.C",))
    [offline] = dbt_column_findings(recorded, "SST-VAL328")
    assert offline.message == "model 'm': column 'email' carries A.B.C and 2 sample_values"
    found = DbtColumn("email", None, "VARCHAR", "dimension", sample_values=("a",))
    tagged = (WarehouseColumn("EMAIL", "TEXT", ("SEC.GOV.EMAIL",)),)
    [enriched] = enrich_findings(found, tagged, {}, "SST-VAL328", Component.SAMPLE_VALUES)
    assert enriched.message == "model 'orders': column 'email' carries SEC.GOV.EMAIL and 1 sample_values"


def test_sst_val328_silent_offline_without_sample_values() -> None:
    assert dbt_column_findings(DbtColumn("email", "e", "VARCHAR", "dimension", pii_tagged=True), "SST-VAL328") == []
