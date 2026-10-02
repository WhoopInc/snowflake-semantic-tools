"""SST-VAL812: sKILL.md exceeds the size budget.

Fires when SKILL.md is over its size budget; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.skill_bundle import (
    SKILL_MD_BUDGET_BYTES,
    build_skill_bundle,
)
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val812_fires() -> None:
    diagnostic = only(
        build_skill_bundle(
            skill(files={"SKILL.md": REF.format(ref="reference/steps.md") + "x" * SKILL_MD_BUDGET_BYTES})
        )[1],
        "SST-VAL812",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "skill 'month-close': SKILL.md is 25685 bytes, over 25600"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val812_silent() -> None:
    assert "SST-VAL812" not in codes(build_skill_bundle(skill())[1])
