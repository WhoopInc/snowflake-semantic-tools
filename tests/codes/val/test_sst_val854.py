"""SST-VAL854: profile registry is not the one Desktop reads.

Fires when profiles publish to a registry Desktop does not read; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.profile import DESKTOP_REGISTRY
from snowflake_semantic_tools.domain.validate.profile import (
    desktop_registry_diagnostics,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val854_fires() -> None:
    diagnostic = only(desktop_registry_diagnostics(QualifiedName.parse("DB.S.REHEARSAL")), "SST-VAL854")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "profiles publish to DB.S.REHEARSAL; CoCo Desktop reads only CORTEX_CODE.CONFIG.PROFILE_REGISTRY"
    )
    assert diagnostic.subject is None


def test_sst_val854_silent() -> None:
    assert "SST-VAL854" not in codes(desktop_registry_diagnostics(QualifiedName.parse(DESKTOP_REGISTRY)))
