"""SST-VAL833: script names a path that flattening renames.

Fires when a script names a nested path the catalog bundle renames; the nearest legitimate input
stays quiet.
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


def test_sst_val833_fires() -> None:
    diagnostic = only(
        flatten_skill(
            skill(
                files={
                    "run.py": 'open("reference/steps.md")\n',
                    "SKILL.md": REF.format(ref="reference/steps.md, then run run.py"),
                }
            )
        )[2],
        "SST-VAL833",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "skill 'month-close': 'run.py' names 'reference/steps.md', which the catalog bundle renames"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val833_silent() -> None:
    assert "SST-VAL833" not in codes(
        flatten_skill(
            skill(files={"run.py": "print(1)\n", "SKILL.md": REF.format(ref="reference/steps.md, then run run.py")})
        )[2]
    )
