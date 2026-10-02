"""SST-RND041: a tool member renders no source statement."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.render.tool import tool_render_checks


def member(kind: str, **fields: object) -> ToolMember:
    return ToolMember("platform", "lookup", kind, ToolOwnership.DEFINE, Origin("tools/t.yml"), "tools/t.yml", **fields)  # type: ignore[arg-type]


def test_sst_rnd041_fires() -> None:
    [diagnostic] = tool_render_checks(member("procedure", body="  \n"), None)
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND041", Severity.ERROR)
    assert diagnostic.message == "tool 'lookup': rendered source query is empty"
    [search] = tool_render_checks(member("cortex_search_service", on_model="docs"), None)
    assert search.code == "SST-RND041"


def test_sst_rnd041_silent() -> None:
    assert tool_render_checks(member("procedure", body="SELECT 1"), None) == ()
    assert tool_render_checks(member("cortex_search_service"), QualifiedName.parse("DB.S.DOCS")) == ()
