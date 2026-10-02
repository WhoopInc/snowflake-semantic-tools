"""SST-VAL321: a CREATE OR ALTER statement would set tags, which that statement cannot do."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.semantic_view import statement_diagnostics

BODY = "SEMANTIC VIEW DB.S.V\n  TABLES (\n    T AS DB.S.T\n  )\n  WITH TAG (\n      DB.S.TIER = 'gold'\n  )"


def test_sst_val321_fires() -> None:
    [found] = statement_diagnostics(f"CREATE OR ALTER {BODY}", artifact="semantic_view:v")
    assert found.severity is Severity.ERROR
    assert found.message == "semantic_view:v: tags would be set by CREATE OR ALTER, which cannot set them"
    assert found.subject == "semantic_view:v"


def test_sst_val321_silent() -> None:
    assert statement_diagnostics(f"CREATE OR REPLACE {BODY}\n  COPY GRANTS", artifact="semantic_view:v") == ()
