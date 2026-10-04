"""SST-VAL808: referenced bundle path does not resolve.

Fires when SKILL.md references a file the bundle lacks; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.skill_flatten import flatten_skill
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val808_fires() -> None:
    diagnostic = only(
        flatten_skill(skill(files={"SKILL.md": REF.format(ref="reference/steps.md and reference/missing.md")}))[2],
        "SST-VAL808",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "skill 'month-close': 'reference/missing.md' is referenced and absent from the bundle"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val808_silent() -> None:
    assert "SST-VAL808" not in codes(flatten_skill(skill())[2])
