"""SST-VAL328: a column with pii_tags carries sample values."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import enrich_findings

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
