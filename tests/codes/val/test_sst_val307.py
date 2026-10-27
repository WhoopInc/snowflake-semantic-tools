"""SST-VAL307: the statement that publishes a view replaces it without preserving grants."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.validate.semantic_view import statement_diagnostics

HEAD = "CREATE OR REPLACE SEMANTIC VIEW DB.S.V\n  TABLES (\n    T AS DB.S.T\n  )"


def test_sst_val307_fires() -> None:
    [found] = statement_diagnostics(HEAD, artifact="semantic_view:v")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "semantic_view:v would be replaced without COPY GRANTS or CREATE OR ALTER"
    assert found.subject == "semantic_view:v"


def test_sst_val307_silent() -> None:
    assert statement_diagnostics(HEAD + "\n  COPY GRANTS", artifact="semantic_view:v") == ()
