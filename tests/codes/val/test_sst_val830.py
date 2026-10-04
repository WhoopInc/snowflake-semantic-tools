"""SST-VAL830: skill reaches neither channel.

Fires when a skill reaches no channel; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.profile import (
    unreached_skills,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    profile,
    profile_catalog,
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val830_fires() -> None:
    diagnostic = only(
        unreached_skills({"month-close": skill()}, profile_catalog(profile(skills=())), catalog_channel=False),
        "SST-VAL830",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "skill 'month-close' is published nowhere"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val830_silent() -> None:
    assert "SST-VAL830" not in codes(
        unreached_skills({"month-close": skill()}, profile_catalog(), catalog_channel=False)
    )
