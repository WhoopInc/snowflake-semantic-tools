"""SST-RND042: a tool member has no value for a field a `tools:` setting could supply."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.tool import tool_render_checks
from tests.helpers.rnd_codes import member

DOCS = QualifiedName.parse("DB.S.DOCS")


def test_sst_rnd042_fires() -> None:
    [diagnostic] = tool_render_checks(member("procedure", body="SELECT 1"), None)
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-RND042", Severity.ERROR, "tool:lookup")
    assert diagnostic.message == "tool 'lookup': warehouse is not set, and no default supplies it"
    assert (
        diagnostic.suggestion
        == "set config.warehouse on the member, or +warehouse in a tools: folder route or the block root"
    )
    search = tool_render_checks(member("cortex_search_service"), DOCS)
    assert [(item.code, item.context["field"]) for item in search] == [
        ("SST-RND042", "warehouse"),
        ("SST-RND042", "target_lag"),
    ]


@pytest.mark.parametrize("field", ["warehouse", "target_lag"])
def test_sst_rnd042_fires_for_each_absent_search_field(field: str) -> None:
    fields = {"warehouse": "WH", "target_lag": "1 hour"}
    del fields[field]
    [diagnostic] = tool_render_checks(member("cortex_search_service", **fields), DOCS)
    assert (diagnostic.code, diagnostic.context["field"]) == ("SST-RND042", field)


def test_sst_rnd042_silent() -> None:
    assert tool_render_checks(member("procedure", body="SELECT 1", warehouse="WH"), None) == ()
    assert tool_render_checks(member("function", body="SELECT 1"), None) == ()
    assert tool_render_checks(member("cortex_search_service", warehouse="WH", target_lag="1 hour"), DOCS) == ()
