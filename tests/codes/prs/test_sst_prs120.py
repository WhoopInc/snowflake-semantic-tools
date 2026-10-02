"""SST-PRS120: a folder holds files of its own while its SKILL.md sits one level too deep."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity

FRONT = "---\nname: {name}\ndescription: {name} steps.\n---\nSteps.\n"


def _found(tmp_path: Path, code: str, files: dict[str, str]) -> list[Diagnostic]:
    for relative, text in files.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    return [item for item in catalog.diagnostics if item.code == code]


def test_sst_prs120_fires(tmp_path: Path) -> None:
    files = {"skills/close/notes.md": "Notes.\n", "skills/close/src/SKILL.md": FRONT.format(name="src")}
    [diagnostic] = _found(tmp_path, "SST-PRS120", files)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill:close: SKILL.md found at skills/close/src/SKILL.md"
    assert diagnostic.subject == "skill:close"


def test_sst_prs120_silent(tmp_path: Path) -> None:
    # A grouping folder holds only skill folders, so its skills are where they belong.
    files = {
        "skills/finance/close/SKILL.md": FRONT.format(name="close"),
        "skills/finance/open/SKILL.md": FRONT.format(name="open"),
    }
    assert _found(tmp_path, "SST-PRS120", files) == []
