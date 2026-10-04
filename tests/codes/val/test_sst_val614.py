"""SST-VAL614: a search service's source relation is created by an artifact that publishes after it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, compile_agents, compile_tools, cross, search_member
from tests.helpers.diagnostic_filters import coded


def _beside(schema: str) -> list[Diagnostic]:
    agents = compile_agents(agent("product_docs"), database="DB", schema=schema)
    return coded(cross(compile_tools((search_member(),)), agents), "SST-VAL614")


def test_sst_val614_fires() -> None:
    [diagnostic] = _beside("MARTS")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "tool member 'docs_search' would publish before 'DB.MARTS.PRODUCT_DOCS', which it indexes"
    )
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val614_silent() -> None:
    assert _beside("AGENTS") == []
