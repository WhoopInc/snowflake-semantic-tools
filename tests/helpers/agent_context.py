"""The smallest agent compile context: one semantic view, no tools, extensions, or other agents."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolCatalog


def agent_context() -> AgentCompileContext:
    """Compile agents into `DB.S` with the `sales` view, warehouse `WH`, and model `model` allowed."""
    return AgentCompileContext(
        semantic_views={"sales": QualifiedName.parse("DB.S.SALES")},
        tools=ToolCatalog((), "dev", frozenset(("dev",))),
        agents={},
        extensions={},
        variables={},
        database="DB",
        schema="S",
        warehouse="WH",
        query_timeout=60,
        orchestration_model="model",
        budget_seconds=None,
        budget_tokens=None,
        tool_not_accessible=None,
        analytical_search=None,
        alias=None,
        allowed_models=frozenset(("model",)),
    )
