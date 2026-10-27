"""SST-VAL622: a defined search service names a column of a model other than its `on:` model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.resolve.tool_column import ColumnSite, tool_column

SITE = ColumnSite("docs_search", "id_column", "tool:docs_search", "tool:docs_search", Origin("tools/t.yml", 4, 3))


def test_sst_val622_fires() -> None:
    column, diagnostic = tool_column("{{ ref('notes', 'doc_id') }}", SITE, referenced=False, on_model="docs")
    assert column is None and diagnostic is not None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-VAL622", Severity.ERROR, SITE.subject)
    assert diagnostic.message == "tool member 'docs_search': id_column names model 'notes', not its on: model 'docs'"


def test_sst_val622_silent() -> None:
    assert tool_column("{{ ref('DOCS', 'doc_id') }}", SITE, referenced=False, on_model="docs") == ("doc_id", None)
    # With no `on:` model to hold it to, which SST-VAL608 reports, any model is taken.
    assert tool_column("{{ ref('notes', 'doc_id') }}", SITE, referenced=False, on_model=None) == ("doc_id", None)
