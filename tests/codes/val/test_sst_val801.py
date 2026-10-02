"""SST-VAL801: skill folder layout is wrong.

Fires when a skill folder is not kebab-case; the nearest legitimate input stays quiet.
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


def test_sst_val801_fires() -> None:
    diagnostic = only(validate_skill_catalog(skill_catalog(skill("Month_Close", declared_name=None))), "SST-VAL801")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "'Month_Close': folder name 'Month_Close' is not lowercase kebab-case of at most 64 characters"
    )
    assert diagnostic.subject == "skill:Month_Close"


def test_sst_val801_silent() -> None:
    assert "SST-VAL801" not in codes(validate_skill_catalog(skill_catalog()))
