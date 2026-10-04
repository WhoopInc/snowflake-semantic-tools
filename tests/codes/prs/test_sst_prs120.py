"""SST-PRS120: a folder holds files of its own while its SKILL.md sits one level too deep."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import skill_tree_findings

FRONT = "---\nname: {name}\ndescription: {name} steps.\n---\nSteps.\n"


def test_sst_prs120_fires(tmp_path: Path) -> None:
    files = {"skills/close/notes.md": "Notes.\n", "skills/close/src/SKILL.md": FRONT.format(name="src")}
    [diagnostic] = skill_tree_findings(tmp_path, "SST-PRS120", files)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill:close: SKILL.md found at skills/close/src/SKILL.md"
    assert diagnostic.subject == "skill:close"


def test_sst_prs120_silent(tmp_path: Path) -> None:
    # A grouping folder holds only skill folders, so its skills are where they belong.
    files = {
        "skills/finance/close/SKILL.md": FRONT.format(name="close"),
        "skills/finance/open/SKILL.md": FRONT.format(name="open"),
    }
    assert skill_tree_findings(tmp_path, "SST-PRS120", files) == []
