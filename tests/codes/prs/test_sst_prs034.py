"""SST-PRS034: a SKILL.md frontmatter omits `name` or `description`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import skill_tree_findings

FRONT = "---\nname: {name}\ndescription: {name} steps.\n---\nSteps.\n"


def test_sst_prs034_fires(tmp_path: Path) -> None:
    [diagnostic] = skill_tree_findings(
        tmp_path, "SST-PRS034", {"skills/close/SKILL.md": "---\nname: close\n---\nSteps.\n"}
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill:close: SKILL.md frontmatter omits 'description'"
    assert diagnostic.subject == "skill:close"


def test_sst_prs034_silent(tmp_path: Path) -> None:
    assert skill_tree_findings(tmp_path, "SST-PRS034", {"skills/close/SKILL.md": FRONT.format(name="close")}) == []
