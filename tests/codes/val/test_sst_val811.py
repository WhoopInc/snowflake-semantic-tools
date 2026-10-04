"""SST-VAL811: flattened bundle exceeds the size budget.

Fires when the bundle is over its size budget; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.skill_bundle import (
    BUNDLE_BUDGET_BYTES,
    build_skill_bundle,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val811_fires() -> None:
    diagnostic = only(
        build_skill_bundle(
            skill(
                files={
                    "reference/big.md": "x" * (BUNDLE_BUDGET_BYTES),
                    "SKILL.md": REF.format(ref="reference/steps.md and reference/big.md"),
                }
            )
        )[1],
        "SST-VAL811",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "'month-close': bundle is 1048691 bytes, over 1048576"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val811_silent() -> None:
    assert "SST-VAL811" not in codes(build_skill_bundle(skill())[1])
