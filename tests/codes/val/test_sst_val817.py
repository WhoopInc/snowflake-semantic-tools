"""SST-VAL817: no publication channel configured.

Fires when the skills block configures no channel; the nearest legitimate input stays quiet.
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


def test_sst_val817_fires() -> None:
    diagnostic = only(validate_config({"skills": {}}), "SST-VAL817")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skills: configures neither the catalog nor the stage channel"
    assert diagnostic.subject == "config:skills"


def test_sst_val817_silent() -> None:
    assert "SST-VAL817" not in codes(validate_config({"skills": {"catalog": {"+bundle_stage": "DB.S.BUNDLES"}}}))
