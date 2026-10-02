"""SST-APL018: a Desktop profile's trees were uploaded and its registry row could not be written."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.publications import compiled_profile, publish_profile
from tests.helpers.recorded_snowflake import RecordedSnowflake


def test_sst_apl018_fires() -> None:
    port = RecordedSnowflake(existing=())
    port.refused = ("MERGE",)
    _, result, _ = publish_profile(port, compiled_profile())
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL018"]
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "profile:analyst: uploaded @DB.S.PROFILES, pointer write failed: recorded refusal: MERGE"
    )
    assert port.uploads


def test_sst_apl018_silent() -> None:
    # A failure before any tree is uploaded is not a half-complete publish.
    port = RecordedSnowflake(existing=())
    port.refused = ("CREATE TABLE",)
    _, result, _ = publish_profile(port, compiled_profile())
    assert "SST-APL018" not in [item.code for item in result.diagnostics] and not port.uploads
