"""SST-RND040: a tool member's kind has no renderer."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render.tool import tool_render_checks
from tests.helpers.rnd_codes import member


def test_sst_rnd040_fires() -> None:
    [diagnostic] = tool_render_checks(member("generic"), None)
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-RND040", Severity.ERROR, "tool:lookup")
    assert diagnostic.message == "tool 'lookup': kind 'generic' has no renderer"


def test_sst_rnd040_silent() -> None:
    assert tool_render_checks(member("stage"), None) == ()
