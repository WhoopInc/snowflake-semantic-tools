"""SST-VAL837: plugin lists a member twice.

Fires when a plugin lists a member twice; the nearest legitimate input stays quiet.
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


def test_sst_val837_fires() -> None:
    diagnostic = only(
        validate_skill_catalog(skill_catalog(plugins=(plugin(members=("month-close", "month-close")),))), "SST-VAL837"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "plugin 'finance-kit': lists 'month-close' more than once"
    assert diagnostic.subject == "plugin:finance-kit"


def test_sst_val837_silent() -> None:
    assert "SST-VAL837" not in codes(validate_skill_catalog(skill_catalog(plugins=(plugin(),))))
