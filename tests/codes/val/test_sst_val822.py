"""SST-VAL822: a profile uploads a tree its registry row has no pointer to.

`build_profile` builds the row from the very trees it uploads, so every tree is reached by a
pointer; compile checks the release before anything uploads, so a tree that would land on
the stage with nothing recording it is reported and the profile does not compile.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.profiles import release_checks
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.publication import registry_pointer_diagnostics
from tests.helpers.publications import PROFILE_STAGE, compiled_profile, skill


def test_sst_val822_fires() -> None:
    upload = f"@{PROFILE_STAGE.sql}/skills/analyst/0123456789ab/"
    [diagnostic] = registry_pointer_diagnostics("profile:analyst", "analyst", (upload,), ())
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL822",
        Severity.ERROR,
        "profile:analyst",
    )
    assert diagnostic.message == f"skill 'analyst': '{upload}' was uploaded with no PROFILE_REGISTRY pointer"


def test_sst_val822_fires_for_a_row_that_drops_a_tree_it_uploads() -> None:
    profile = compiled_profile(skill())
    release = profile.release
    [tree] = [item for item in release.trees if item.kind == "skills"]
    # The row still names the tree, so VERSION covers it, but no column points Desktop at it.
    row = {**release.row, "SKILL_REPOS": [], "DESCRIPTION": tree.prefix}
    found = release_checks(replace(profile, release=replace(release, row=row)))
    assert [(item.code, item.context["path"]) for item in found] == [
        ("SST-VAL822", f"@{PROFILE_STAGE.sql}/{tree.prefix}")
    ]


def test_sst_val822_silent() -> None:
    profile = compiled_profile(skill())
    assert release_checks(profile) == ()
