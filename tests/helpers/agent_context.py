"""An agent compile context for tests: one semantic view, one extension, one skill, and one plugin."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.compile.agents import AgentCompileContext, ExtensionPin
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership

SEMANTICS = ExtensionPin("skill:semantics", QualifiedName.parse("DB.S.SEMANTICS"), "SST_ABCDEF012345", ("semantics",))
TOOLKIT = ExtensionPin("plugin:toolkit", QualifiedName.parse("DB.S.TOOLKIT"), "SST_0123456789AB", ("semantics",))


def search_member(kind: str = "cortex_search_service") -> ToolMember:
    """A referenced tool member `platform.search` of the kind given, with a relation for `dev`."""
    return ToolMember(
        "platform",
        "search",
        kind,
        ToolOwnership.REFERENCE,
        Origin("tools/platform.yml"),
        "tools/platform.yml",
        description="Search the docs.",
        relations=MappingProxyType({"dev": "DB.S.SEARCH"}),
    )


def tools(*members: ToolMember) -> ToolCatalog:
    """A tool catalog of one `platform` group holding `members`, compiled for `dev`."""
    group = ToolGroup("platform", Origin("tools/platform.yml"), "tools/platform.yml", members=members)
    return ToolCatalog((group,), "dev", frozenset(("dev",)))


def agent_context(
    *,
    catalog: ToolCatalog | None = None,
    agents: dict[str, QualifiedName] | None = None,
) -> AgentCompileContext:
    """The context agents compile against: view `sales`, extension `vendor-pack`, skill and plugin."""
    return AgentCompileContext(
        semantic_views={"sales": QualifiedName.parse("DB.S.SALES")},
        tools=catalog or tools(),
        agents=agents or {},
        extensions={"vendor-pack": QualifiedName.parse("DB.EXT.VENDOR_PACK")},
        variables={"sha_version": "0000000"},
        database="DB",
        schema="S",
        warehouse="WH",
        query_timeout=60,
        orchestration_model="model",
        budget_seconds=120,
        budget_tokens=16000,
        tool_not_accessible="reject",
        analytical_search=False,
        alias="promoted",
        allowed_models=frozenset(("model",)),
        skills={"semantics": SEMANTICS},
        plugins={"toolkit": TOOLKIT},
    )


def docs_catalog() -> ToolCatalog:
    """A tool catalog for `dev` with one defined search service `platform.docs`."""
    member = ToolMember(
        "platform", "docs", "cortex_search_service", ToolOwnership.DEFINE, Origin("tools/p.yml", 1), "tools/p.yml"
    )
    group = ToolGroup("platform", Origin("tools/p.yml", 1), "tools/p.yml", members=(member,))
    return ToolCatalog((group,), "dev", frozenset(("dev",)))
