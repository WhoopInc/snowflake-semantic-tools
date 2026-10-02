"""SST-CFG017: a `{{ tool(...) }}` in a configuration value names no declared tool group and member."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.config import config_tool_references


def _catalog() -> ToolCatalog:
    member = ToolMember(
        "platform", "docs", "cortex_search_service", ToolOwnership.DEFINE, Origin("tools/p.yml", 1), "tools/p.yml"
    )
    group = ToolGroup("platform", Origin("tools/p.yml", 1), "tools/p.yml", members=(member,))
    return ToolCatalog((group,), "dev", frozenset(("dev",)))


def test_sst_cfg017_fires() -> None:
    tree = {"agents": {"+alias": "{{ tool('platform', 'missing') }}"}}
    [diagnostic] = config_tool_references(tree, _catalog(), file="sst_config.yml")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG017", Severity.ERROR)
    assert diagnostic.message == "{ tool('platform','missing') } does not resolve"
    assert diagnostic.subject == "config:agents.+alias"


def test_sst_cfg017_silent() -> None:
    assert config_tool_references({"agents": {"+alias": "{{ tool('Platform', 'docs') }}"}}, _catalog()) == ()
