"""SST-VAL857: file name cannot be published to a stage.

Fires when a skill file has a name a stage rejects; the nearest legitimate input stays quiet.
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


def test_sst_val857_fires() -> None:
    diagnostic = only(validate_skill_catalog(skill_catalog(skill(files={"reference/my steps.md": "x"}))), "SST-VAL857")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "skill:month-close: reference/my steps.md cannot be staged, because 'my steps.md' is not "
        "made only of letters, digits, '.', '_', '-', or '$'"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val857_silent() -> None:
    assert "SST-VAL857" not in codes(validate_skill_catalog(skill_catalog(skill(files={"reference/my-steps.md": "x"}))))
