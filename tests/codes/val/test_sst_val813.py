"""SST-VAL813: bundled file is never referenced.

Fires when a bundled file is never referenced; the nearest legitimate input stays quiet.
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


def test_sst_val813_fires() -> None:
    diagnostic = only(flatten_skill(skill(files={"notes/extra.md": "Extra.\n"}))[2], "SST-VAL813")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == ("skill 'month-close': 'notes/extra.md' is never referenced from the skill's Markdown")
    assert diagnostic.subject == "skill:month-close"


def test_sst_val813_silent() -> None:
    assert "SST-VAL813" not in codes(flatten_skill(skill())[2])
