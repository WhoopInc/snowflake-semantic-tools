"""SST-VAL870: a compiled CoWork profile relies on a Snowflake private preview."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import PROFILE_ORIGIN, compile_profiles, cowork_profile, described_agent
from tests.helpers.diagnostic_filters import coded


def test_sst_val870_fires() -> None:
    [diagnostic] = coded(compile_profiles(cowork_profile(), agents=(described_agent(),)).diagnostics, "SST-VAL870")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "cowork profile 'everyone' relies on CoWork profiles, a Snowflake private preview"
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", PROFILE_ORIGIN)


def test_sst_val870_silent() -> None:
    # A profile that does not compile, here one routing to no agent the project has, is not noted.
    assert coded(compile_profiles(cowork_profile()).diagnostics, "SST-VAL870") == []
