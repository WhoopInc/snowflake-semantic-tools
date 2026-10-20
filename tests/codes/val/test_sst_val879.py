"""SST-VAL879: a project agent and a CoWork profile publish to one schema."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import PROFILE_ORIGIN, compile_profiles, cowork_profile, described_agent
from tests.helpers.diagnostic_filters import coded


def test_sst_val879_fires() -> None:
    # With neither block placing them, both publish to the target's own DB.SCH.
    result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree={})
    [diagnostic] = coded(result.diagnostics, "SST-VAL879")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "cowork profile 'everyone' publishes to DB.SCH, where agent 'sales' publishes too"
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", PROFILE_ORIGIN)


def test_sst_val879_silent() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(),))
    assert coded(result.diagnostics, "SST-VAL879") == []
