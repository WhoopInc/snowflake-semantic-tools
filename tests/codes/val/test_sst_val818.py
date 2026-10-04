"""SST-VAL818: flatten settings are wrong for a channel.

Fires when the catalog channel is not flattened; the nearest legitimate input stays quiet.
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


def test_sst_val818_fires() -> None:
    diagnostic = only(
        validate_config({"skills": {"catalog": {"+bundle_stage": "DB.S.BUNDLES", "+flatten": False}}}), "SST-VAL818"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'skills.catalog.+flatten' is false; it must be true"
    assert diagnostic.subject == "config:skills.catalog.+flatten"


def test_sst_val818_silent() -> None:
    assert "SST-VAL818" not in codes(
        validate_config({"skills": {"catalog": {"+bundle_stage": "DB.S.BUNDLES", "+flatten": True}}})
    )
