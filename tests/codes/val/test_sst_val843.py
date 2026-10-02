"""SST-VAL843: two artifacts publish to one Snowflake name.

Fires when a skill and a plugin publish to one name; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.targets import shared_targets
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val843_fires() -> None:
    diagnostic = only(
        shared_targets((("skill:kit", "skill:kit", TARGET), ("plugin:kit", "plugin:kit", TARGET))), "SST-VAL843"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "skill:kit and plugin:kit both publish to DB.S.KIT"
    assert diagnostic.subject == "plugin:kit"


def test_sst_val843_silent() -> None:
    assert "SST-VAL843" not in codes(
        shared_targets(
            (("skill:kit", "skill:kit", TARGET), ("plugin:kit", "plugin:kit", QualifiedName.parse("DB.S.KIT_PLUGIN")))
        )
    )
