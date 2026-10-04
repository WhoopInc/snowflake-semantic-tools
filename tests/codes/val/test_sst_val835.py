"""SST-VAL835: plugin member is not a project skill.

Fires when a plugin lists a skill the project lacks; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    plugin,
    skill,
    skill_catalog,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val835_fires() -> None:
    diagnostic = only(
        validate_skill_catalog(skill_catalog(plugins=(plugin(members=("month-close", "ghost")),))), "SST-VAL835"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "plugin 'finance-kit': member 'ghost' is not a skill in this project"
    assert diagnostic.subject == "plugin:finance-kit"


def test_sst_val835_silent() -> None:
    assert "SST-VAL835" not in codes(validate_skill_catalog(skill_catalog(plugins=(plugin(),))))
