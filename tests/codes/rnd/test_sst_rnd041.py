"""SST-RND041: a tool member renders no source statement."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.tool import tool_render_checks
from tests.helpers.rnd_codes import member


def test_sst_rnd041_fires() -> None:
    [diagnostic] = tool_render_checks(member("procedure", body="  \n", warehouse="WH"), None)
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND041", Severity.ERROR)
    assert diagnostic.message == "tool 'lookup': rendered source query is empty"
    [search] = tool_render_checks(member("cortex_search_service", on_model="docs", warehouse="WH", target_lag="1 hour"), None)
    assert search.code == "SST-RND041"


def test_sst_rnd041_silent() -> None:
    assert tool_render_checks(member("procedure", body="SELECT 1", warehouse="WH"), None) == ()
    assert tool_render_checks(member("cortex_search_service", warehouse="WH", target_lag="1 hour"), QualifiedName.parse("DB.S.DOCS")) == ()
