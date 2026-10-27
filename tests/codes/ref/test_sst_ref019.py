"""SST-REF019: a referenced tool member's relation is not a three-part name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ref_codes import procedure_relation_findings


def test_sst_ref019_fires() -> None:
    [diagnostic] = procedure_relation_findings({"dev": "DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}, "SST-REF019")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "'DEV.LOOKUP' is not a three-part fully-qualified name"
    assert diagnostic.subject == "tool:lookup"


def test_sst_ref019_silent() -> None:
    assert procedure_relation_findings({"dev": "DB.DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}, "SST-REF019") == []
