"""SST-VAL613: a declared column is absent from the live search service."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import compile_tools, observe, search_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _live(columns: str) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.descriptions["CORTEX SEARCH SERVICE DB.S.DOCS_SEARCH"] = {"columns": columns}
    return coded(observe(port, compile_tools((search_member(),))), "SST-VAL613")


def test_sst_val613_fires() -> None:
    [diagnostic] = _live("DOC_ID, BODY")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "tool member 'docs_search': column 'DOC_NAME' is absent from the live object"
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val613_silent() -> None:
    assert _live("doc_id,doc_name,body") == []
