"""SST-PLN028: a profile registry row carries a VERSION SST did not last write."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.lifecycle_codes import REGISTRY, analyst_profile, lifecycle_state, plan_profiles
from tests.helpers.snowflake_fake import FakeSnowflake


def _planned(recorded_version: str) -> list[Diagnostic]:
    port = FakeSnowflake(existing=())
    port.ensure_profile_registry(REGISTRY)
    port.merge_profile_row(REGISTRY, {"CONFIG_NAME": "analyst", "VERSION": "66.00000"}, expected_version=None)
    owned = AppliedEntry(
        "f", REGISTRY.sql, "now", "run", "applied", "f", "m", component_fingerprints=(("version", recorded_version),)
    )
    changeset = plan_profiles(port, analyst_profile(), lifecycle_state({"profile:analyst": owned}))
    return [item for item in changeset.diagnostics if item.code == "SST-PLN028"]


def test_sst_pln028_fires() -> None:
    [diagnostic] = _planned("SST_OLD")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "profile:analyst: registry row 'analyst' has VERSION 66.0, and SST last wrote SST_OLD"


def test_sst_pln028_silent() -> None:
    assert _planned("66.0") == []
