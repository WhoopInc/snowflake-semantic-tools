"""SST-CFG018: a declared tool member is referenced by no agent's `{{ tool(...) }}`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.config import unreferenced_tool_members


def _catalog() -> ToolCatalog:
    member = ToolMember(
        "platform", "docs", "cortex_search_service", ToolOwnership.DEFINE, Origin("tools/p.yml", 1), "tools/p.yml"
    )
    group = ToolGroup("platform", Origin("tools/p.yml", 1), "tools/p.yml", members=(member,))
    return ToolCatalog((group,), "dev", frozenset(("dev",)))


def test_sst_cfg018_fires() -> None:
    [diagnostic] = unreferenced_tool_members(_catalog(), [("other", "docs")])
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG018", Severity.WARNING)
    assert diagnostic.message == "tool member 'platform.docs' is referenced by nothing"
    assert (diagnostic.subject, diagnostic.origin) == ("tool_group:platform", Origin("tools/p.yml", 1))


def test_sst_cfg018_silent() -> None:
    assert unreferenced_tool_members(_catalog(), [("platform", "Docs")]) == ()
    assert unreferenced_tool_members(_catalog(), [("docs",)]) == ()
