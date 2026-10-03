"""SST-VAL823: what a profile's VERSION digests does not name every tree the profile ships.

The row names each tree by its digest-named prefix and VERSION digests the row, so a change
to any tree is a new VERSION; compile checks the release before anything uploads, and a tree
the digest would not cover is reported and the profile does not compile.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.compile import profiles
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.profile import DesktopProfile, ProfileCatalog
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.validate.publication import version_coverage_diagnostics
from tests.helpers.publications import PROFILE_REGISTRY, PROFILE_STAGE, compiled_profile, skill


def test_sst_val823_fires() -> None:
    [diagnostic] = version_coverage_diagnostics("profile:analyst", "analyst", ("hooks/analyst/0123456789ab/",), "{}")
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL823",
        Severity.ERROR,
        "profile:analyst",
    )
    assert diagnostic.message == "skill 'analyst': hash omits hooks/analyst/0123456789ab/"


def test_sst_val823_fires_for_each_tree_when_the_version_digests_less_than_the_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = compiled_profile(skill())
    monkeypatch.setattr(profiles, "version_input", lambda row: "{}")
    found = profiles.release_checks(profile)
    assert [item.code for item in found] == ["SST-VAL823"] * len(profile.release.trees)


def test_sst_val823_keeps_the_profile_from_compiling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(profiles, "version_input", lambda row: "{}")
    catalog = ProfileCatalog(
        (
            DesktopProfile(
                name="analyst",
                directory="profiles/analyst",
                description="Analyst.",
                owner_team="Data",
                skills=("month-close",),
                mcp_servers=(),
                hooks=(),
                prompt="Be careful.\n",
                origin=Origin("profiles/analyst/profile.yml", 1),
            ),
        ),
        None,
        (),
        (),
    )
    channel = profiles.DesktopChannel(PROFILE_STAGE, PROFILE_REGISTRY)
    result = profiles.CompileProfiles(catalog, SkillCatalog((skill(),)), channel, catalog_channel=True).run_result()
    assert result.compiled == ()
    assert {item.subject for item in result.diagnostics if item.code == "SST-VAL823"} == {"profile:analyst"}


def test_sst_val823_silent() -> None:
    profile = compiled_profile(skill())
    assert [item.code for item in profiles.release_checks(profile)] == []
