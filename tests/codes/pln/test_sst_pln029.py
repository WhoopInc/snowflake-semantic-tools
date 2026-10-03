"""SST-PLN029: the profile registry table lacks a column SST writes, or types one differently."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.lifecycle_codes import REGISTRY, analyst_profile, lifecycle_state, plan_profiles
from tests.helpers.snowflake_fake import PROFILE_REGISTRY_SHAPE, FakeSnowflake


def test_sst_pln029_fires() -> None:
    port = FakeSnowflake(existing=())
    port.tables[REGISTRY.sql] = (("CONFIG_NAME", "VARCHAR"), ("VERSION", "NUMBER(38,0)"))
    [diagnostic] = plan_profiles(port, analyst_profile(), lifecycle_state()).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN029", Severity.ERROR)
    assert diagnostic.message.startswith("profile:analyst: DB.S.PROFILE_REGISTRY lacks")


def test_sst_pln029_silent() -> None:
    port = FakeSnowflake(existing=())
    port.tables[REGISTRY.sql] = PROFILE_REGISTRY_SHAPE
    assert "SST-PLN029" not in [
        item.code for item in plan_profiles(port, analyst_profile(), lifecycle_state()).diagnostics
    ]
