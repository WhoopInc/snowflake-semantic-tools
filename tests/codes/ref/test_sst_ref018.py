"""SST-REF018: a referenced tool member's `relations:` has no entry for the target being compiled."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ref_codes import procedure_relation_findings


def test_sst_ref018_fires() -> None:
    [diagnostic] = procedure_relation_findings({"prod": "DB.PROD.LOOKUP"}, "SST-REF018")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ tool('platform', 'lookup') } has no entry for target 'dev'"
    assert diagnostic.subject == "tool:lookup"


def test_sst_ref018_silent() -> None:
    assert procedure_relation_findings({"dev": "DB.DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}, "SST-REF018") == []
