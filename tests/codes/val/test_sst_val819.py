"""SST-VAL819: stage auto_compress is enabled.

Fires when the stage channel compresses uploads; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.config import validate_config
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val819_fires() -> None:
    diagnostic = only(
        validate_config({"skills": {"stage": {"+stage": "DB.S.REG", "+auto_compress": True}}}), "SST-VAL819"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'skills.stage.+auto_compress' is true; it must be false"
    assert diagnostic.subject == "config:skills.stage.+auto_compress"


def test_sst_val819_silent() -> None:
    assert "SST-VAL819" not in codes(
        validate_config({"skills": {"stage": {"+stage": "DB.S.REG", "+auto_compress": False}}})
    )
