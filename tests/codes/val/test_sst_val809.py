"""SST-VAL809: flattened file names collide.

Fires when two authored paths flatten to one name; the nearest legitimate input stays quiet.
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


def test_sst_val809_fires() -> None:
    diagnostic = only(flatten_skill(skill(files={"reference__steps.md": "Same.\n"}))[2], "SST-VAL809")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "skill 'month-close': 'reference/steps.md' and 'reference__steps.md' flatten to one name"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val809_silent() -> None:
    assert "SST-VAL809" not in codes(flatten_skill(skill(files={"reference_steps.md": "Other.\n"}))[2])
