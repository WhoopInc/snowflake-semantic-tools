"""SST-VAL815: bundled script reads a credential or an absolute local path.

Fires when a bundled script names an absolute local path; the nearest legitimate input stays quiet.
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


def test_sst_val815_fires() -> None:
    diagnostic = only(
        flatten_skill(
            skill(
                files={
                    "run.py": 'open("/Users/.../data.csv")\n',
                    "SKILL.md": REF.format(ref="reference/steps.md, then run run.py"),
                }
            )
        )[2],
        "SST-VAL815",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "skill 'month-close': 'run.py' contains the absolute path '/Users/.../data.csv'"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val815_silent() -> None:
    assert "SST-VAL815" not in codes(
        flatten_skill(
            skill(
                files={
                    "run.py": 'open("data.csv")\n',
                    "SKILL.md": REF.format(ref="reference/steps.md, then run run.py"),
                }
            )
        )[2]
    )
