"""SST-VAL808: a Markdown link names no file of the skill.

Fires when SKILL.md links to a file the skill lacks; a path mentioned in prose or a code block,
or a link to a file in a subfolder, stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.skill.model import SkillCatalog
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog
from snowflake_semantic_tools.domain.validate.skill_files import check_skill_files
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"


def test_sst_val808_fires() -> None:
    diagnostic = only(
        check_skill_files(
            skill(files={"SKILL.md": REF.format(ref="[steps](reference/steps.md) and [gone](reference/missing.md)")})
        ),
        "SST-VAL808",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "skill 'month-close': the link to 'reference/missing.md' names no file or folder of the skill"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val808_silent() -> None:
    text = REF.format(ref="[steps](reference/steps.md), steps.md, [notes](reference/), and scripts/my_script.py")
    assert "SST-VAL808" not in codes(check_skill_files(skill(files={"SKILL.md": text})))


def test_sst_val808_resolves_from_the_linking_file() -> None:
    files: dict[str, bytes | str] = {
        "SKILL.md": REF.format(ref="[steps](reference/steps.md)"),
        "reference/steps.md": "See [the template](../assets/template.md) and [it again](template.md).\n",
        "assets/template.md": "Template.\n",
    }
    found = check_skill_files(skill(files=files))
    assert [item.context["path"] for item in found if item.code == "SST-VAL808"] == ["template.md"]


def test_sst_val808_fires_for_a_skill_no_channel_ships() -> None:
    # The catalog checks run for every skill validate reads, not only those a bundle builds.
    broken = skill(files={"SKILL.md": REF.format(ref="[gone](reference/missing.md)")})
    assert codes(validate_skill_catalog(SkillCatalog((broken,)))).count("SST-VAL808") == 1
