"""SST-VAL844: profile names a skill the project does not have.

Fires when a profile names a skill the project lacks; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.profile import (
    validate_profile_catalog,
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


def test_sst_val844_fires() -> None:
    diagnostic = only(validate_profile_catalog(profile_catalog(profile(skills=("ghost",))), SKILLS), "SST-VAL844")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "profile 'analyst': skill 'ghost' is not a skill in this project"
    assert diagnostic.subject == "profile:analyst"


def test_sst_val844_silent() -> None:
    assert "SST-VAL844" not in codes(validate_profile_catalog(profile_catalog(), SKILLS))
