"""SST-VAL821: stage layout is not by_type.

Fires when the stage layout is not by_type; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.config import validate_config
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val821_fires() -> None:
    diagnostic = only(validate_config({"skills": {"stage": {"+stage": "DB.S.REG", "+layout": "flat"}}}), "SST-VAL821")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'skills.stage.+layout' value 'flat' is outside by_type"
    assert diagnostic.subject == "config:skills.stage.+layout"


def test_sst_val821_silent() -> None:
    assert "SST-VAL821" not in codes(
        validate_config({"skills": {"stage": {"+stage": "DB.S.REG", "+layout": "by_type"}}})
    )
