"""SST-VAL855: profile includes a skill or plugin with errors.

A profile carrying a skill that compile blocked cannot publish; one carrying healthy skills can.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompileCocoDesktopProfiles, DesktopChannel
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import profile, profile_catalog, skill_catalog

CHANNEL = DesktopChannel(QualifiedName.parse("DB.S.PROFILES"), QualifiedName.parse("DB.S.PROFILE_REGISTRY"))


def test_sst_val855_fires() -> None:
    result = CompileCocoDesktopProfiles(
        profile_catalog(profile()),
        skill_catalog(),
        CHANNEL,
        catalog_published=frozenset(),
        blocked_skills=frozenset(("month-close",)),
    ).run_result()
    diagnostic = only(result.diagnostics, "SST-VAL855")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == "CoCo Desktop profile 'analyst': skill 'month-close' has errors, so the profile cannot publish"
    )
    assert diagnostic.subject == "coco_desktop_profile:analyst"
    assert result.compiled == ()


def test_sst_val855_silent() -> None:
    result = CompileCocoDesktopProfiles(
        profile_catalog(profile()), skill_catalog(), CHANNEL, catalog_published=frozenset()
    ).run_result()
    assert "SST-VAL855" not in codes(result.diagnostics)
    assert [item.artifact_key for item in result.compiled] == ["coco_desktop_profile:analyst"]
