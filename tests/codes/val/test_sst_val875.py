"""SST-VAL875: a CoWork profile routes to an agent marked deprecated."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import compile_profiles, cowork_profile, described_agent, subagent
from tests.helpers.diagnostic_filters import coded


def test_sst_val875_fires() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(deprecated=True),))
    [diagnostic] = coded(result.diagnostics, "SST-VAL875")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "cowork profile 'everyone': spec.subagents[0] agent('sales') is deprecated"
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", subagent().origin)


def test_sst_val875_silent() -> None:
    assert coded(compile_profiles(cowork_profile(), agents=(described_agent(),)).diagnostics, "SST-VAL875") == []
