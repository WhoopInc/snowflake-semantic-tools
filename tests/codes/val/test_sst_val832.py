"""SST-VAL832: skill name is not globally unique across extensions.

Fires when two skills take one extension name; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    skill,
    skill_catalog,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val832_fires() -> None:
    diagnostic = only(
        validate_skill_catalog(skill_catalog(skill(), skill(directory="skills/ops/month-close"))), "SST-VAL832"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "'skills/ops/month-close' collides with 'skills/finance/month-close' as one extension name"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val832_silent() -> None:
    assert "SST-VAL832" not in codes(validate_skill_catalog(skill_catalog(skill(), skill("year-close"))))
