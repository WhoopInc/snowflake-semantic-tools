"""SST-VAL872: a CoWork profile's `tool()` subagent names a tool member that is not an agent."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import agent_member, compile_profiles, cowork_profile, subagent
from tests.helpers.diagnostic_filters import coded


def test_sst_val872_fires() -> None:
    search = agent_member("docs", kind="cortex_search_service")
    result = compile_profiles(cowork_profile("everyone", subagent("docs", "tool")), members=(search,))
    [diagnostic] = coded(result.diagnostics, "SST-VAL872")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "cowork profile 'everyone': spec.subagents[0] tool('docs') is a cortex_search_service member, not an agent"
    )
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", subagent().origin)


def test_sst_val872_silent() -> None:
    entry = subagent("partner", "tool", display_name="Partner", description="Partner deals.")
    result = compile_profiles(cowork_profile("everyone", entry), members=(agent_member(),))
    assert coded(result.diagnostics, "SST-VAL872") == []
