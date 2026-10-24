"""SST-VAL810: a path names a skill file from the repository root.

Fires when SKILL.md names one of its own files by its repository path; the path from the skill
folder stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.skill_files import check_skill_files
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"


def test_sst_val810_fires() -> None:
    diagnostic = only(
        check_skill_files(skill(files={"SKILL.md": REF.format(ref="skills/finance/month-close/reference/steps.md")})),
        "SST-VAL810",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "skill 'month-close': 'skills/finance/month-close/reference/steps.md' names 'reference/steps.md' "
        "by its path from the repository root"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val810_silent() -> None:
    assert "SST-VAL810" not in codes(check_skill_files(skill(files={"SKILL.md": REF.format(ref="reference/steps.md")})))
