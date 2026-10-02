"""SST-VAL845: profile repeats a shared skill or command.

Fires when a profile repeats a skill the shared layer carries; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.profile import (
    validate_profile_catalog,
)
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    profile,
    profile_catalog,
    shared,
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val845_fires() -> None:
    diagnostic = only(
        validate_profile_catalog(profile_catalog(shared=shared(skills=("month-close",))), SKILLS), "SST-VAL845"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "profile 'analyst': skill 'month-close' already reaches every profile through shared/"
    )
    assert diagnostic.subject == "profile:analyst"


def test_sst_val845_silent() -> None:
    assert "SST-VAL845" not in codes(
        validate_profile_catalog(profile_catalog(profile(skills=()), shared=shared(skills=("month-close",))), SKILLS)
    )
