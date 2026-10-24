"""SST-VAL880: a project agent publishes to SNOWFLAKE_INTELLIGENCE.PROFILES, where CoWork lists profiles."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import AGENT_ORIGIN, compile_profiles, described_agent
from tests.helpers.diagnostic_filters import coded


def test_sst_val880_fires() -> None:
    tree = {"agents": {"+database": "SNOWFLAKE_INTELLIGENCE", "+schema": "PROFILES"}}
    [diagnostic] = coded(compile_profiles(agents=(described_agent(),), tree=tree).diagnostics, "SST-VAL880")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales' publishes to SNOWFLAKE_INTELLIGENCE.PROFILES, where CoWork lists every agent as a CoWork profile"
    )
    assert (diagnostic.subject, diagnostic.origin) == ("agent:sales", AGENT_ORIGIN)


def test_sst_val880_silent() -> None:
    tree = {"agents": {"+database": "SNOWFLAKE_INTELLIGENCE", "+schema": "AGENTS"}}
    assert coded(compile_profiles(agents=(described_agent(),), tree=tree).diagnostics, "SST-VAL880") == []
