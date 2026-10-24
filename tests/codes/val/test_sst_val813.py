"""SST-VAL813: bundled file is never referenced.

Fires when a file of the skill is never named; a file named by its unique file name, or one in a
nested skill's folder, stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.skill_files import check_skill_files
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"


def test_sst_val813_fires() -> None:
    diagnostic = only(check_skill_files(skill(files={"notes/extra.md": "Extra.\n"})), "SST-VAL813")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == ("skill 'month-close': 'notes/extra.md' is never referenced from the skill's Markdown")
    assert diagnostic.subject == "skill:month-close"


def test_sst_val813_silent() -> None:
    assert "SST-VAL813" not in codes(check_skill_files(skill()))


def test_sst_val813_counts_a_file_named_by_its_unique_file_name() -> None:
    files: dict[str, bytes | str] = {
        "SKILL.md": REF.format(ref="reference/steps.md, then run eppo_stats.py"),
        "scripts/eppo_stats.py": "x = 1\n",
    }
    assert "SST-VAL813" not in codes(check_skill_files(skill(files=files)))


def test_sst_val813_leaves_a_nested_skill_to_itself() -> None:
    files: dict[str, bytes | str] = {
        "eda-agent/SKILL.md": "---\nname: eda-agent\ndescription: EDA.\n---\nSteps.\n",
        "eda-agent/notes.md": "N.\n",
    }
    assert "SST-VAL813" not in codes(check_skill_files(skill(files=files, nested=("eda-agent",))))
