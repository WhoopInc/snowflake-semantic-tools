"""SST-REF050: a field read only as a template call holds a bare value."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.resolve.tool_column import ColumnSite, tool_column

SITE = ColumnSite("docs_search", "search_column", "tool:docs_search", "tool:docs_search", Origin("tools/t.yml", 4, 3))


def test_sst_ref050_fires() -> None:
    column, diagnostic = tool_column("body", SITE, referenced=False, on_model="docs")
    assert column is None and diagnostic is not None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-REF050", Severity.ERROR, SITE.subject)
    assert diagnostic.message == "tool:docs_search: search_column is 'body'; it takes a {{ ref(...) }} call"


def test_sst_ref050_silent() -> None:
    assert tool_column("{{ ref('docs', 'body') }}", SITE, referenced=False, on_model="docs") == ("body", None)
    # A referenced member's columns are the object's own, written bare.
    assert tool_column("body", SITE, referenced=True, on_model=None) == ("body", None)
