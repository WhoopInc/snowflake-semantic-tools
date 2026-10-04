"""SST-VAL855: profile includes a skill or plugin with errors.

A profile carrying a skill that compile blocked cannot publish; one carrying healthy skills can.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.profiles import CompileProfiles, DesktopChannel
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import profile, profile_catalog, skill_catalog

CHANNEL = DesktopChannel(QualifiedName.parse("DB.S.PROFILES"), QualifiedName.parse("DB.S.PROFILE_REGISTRY"))


def test_sst_val855_fires() -> None:
    result = CompileProfiles(
        profile_catalog(profile()),
        skill_catalog(),
        CHANNEL,
        catalog_channel=False,
        blocked_skills=frozenset(("month-close",)),
    ).run_result()
    diagnostic = only(result.diagnostics, "SST-VAL855")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "profile 'analyst': skill 'month-close' has errors, so the profile cannot publish"
    assert diagnostic.subject == "profile:analyst"
    assert result.compiled == ()


def test_sst_val855_silent() -> None:
    result = CompileProfiles(profile_catalog(profile()), skill_catalog(), CHANNEL, catalog_channel=False).run_result()
    assert "SST-VAL855" not in codes(result.diagnostics)
    assert [item.artifact_key for item in result.compiled] == ["profile:analyst"]
