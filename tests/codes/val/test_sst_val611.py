"""SST-VAL611: a gated agent reads a search service that pins no embedding model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import (
    agent,
    catalog,
    compile_agents,
    compile_tools,
    cross,
    evaluation,
    found,
    search_member,
    search_tool,
)


def _gated(embedding_model: str | None) -> list[Diagnostic]:
    tools = catalog(search_member(embedding_model=embedding_model))
    model = agent("sales_agent", search_tool())
    results = (compile_tools(tools.members), compile_agents(model, tools=tools))
    return found(cross(*results, evals=(evaluation(model, "Which item is vegan?"),)), "SST-VAL611")


def test_sst_val611_fires() -> None:
    [diagnostic] = _gated(None)
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "tool member 'docs_search' declares embedding_model: null and is read by a gated agent"
    )
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val611_silent() -> None:
    assert _gated("snowflake-arctic-embed-m-v1.5") == []
