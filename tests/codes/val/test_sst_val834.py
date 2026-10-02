"""SST-VAL834: bundle exceeds the extension scan limits.

Fires when a bundle holds more files than a scan reads; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.skill_bundle import (
    SCAN_MAX_FILES,
    build_skill_bundle,
)
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val834_fires() -> None:
    diagnostic = only(
        build_skill_bundle(
            skill(
                files={
                    "SKILL.md": REF.format(ref=", ".join(f"reference/{i}.md" for i in range(SCAN_MAX_FILES))),
                    **{f"reference/{i}.md": "x" for i in range(SCAN_MAX_FILES)},
                }
            )
        )[1],
        "SST-VAL834",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "'month-close': 52 files, over the limit of 50"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val834_silent() -> None:
    assert "SST-VAL834" not in codes(build_skill_bundle(skill())[1])
