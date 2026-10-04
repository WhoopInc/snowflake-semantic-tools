"""SST-REF023: a mutable group's reference resolves to one object for both dev and prod."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ref_codes import procedure_relation_findings


def test_sst_ref023_fires() -> None:
    [diagnostic] = procedure_relation_findings({"dev": "DB.S.LOOKUP", "prod": "DB.S.LOOKUP"}, "SST-REF023")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "{ tool('platform', 'lookup') } resolves to DB.S.LOOKUP for both dev and prod"
    assert diagnostic.subject == "tool:lookup"


def test_sst_ref023_silent() -> None:
    assert procedure_relation_findings({"dev": "DB.DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}, "SST-REF023") == []
