"""SST-VAL810: flattening did not rewrite a subdirectory reference.

Fires when SKILL.md names a bundled file by its repository path; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.skill_flatten import flatten_skill
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val810_fires() -> None:
    diagnostic = only(
        flatten_skill(skill(files={"SKILL.md": REF.format(ref="skills/finance/month-close/reference/steps.md")}))[2],
        "SST-VAL810",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "skill 'month-close': 'skills/finance/month-close/reference/steps.md' does not resolve "
        "inside the published bundle"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val810_silent() -> None:
    assert "SST-VAL810" not in codes(flatten_skill(skill(files={"SKILL.md": REF.format(ref="reference/steps.md")}))[2])
