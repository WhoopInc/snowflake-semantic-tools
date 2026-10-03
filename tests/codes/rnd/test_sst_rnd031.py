"""SST-RND031: a skill's rendered SKILL.md gives no instructions after its frontmatter."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.render.skill_bundle import build_skill_bundle
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog


def skill(body: str) -> Skill:
    content = f"---\nname: close\n---\n{body}".encode()
    return Skill(
        "close", "skills/close", "close", "Close.", body, (SkillFile("SKILL.md", content),), Origin("SKILL.md")
    )


def test_sst_rnd031_fires() -> None:
    _, diagnostics = build_skill_bundle(skill("\n  \n"))
    [diagnostic] = (item for item in diagnostics if item.code == "SST-RND031")
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-RND031", Severity.WARNING, "skill:close")
    assert diagnostic.message == "skill 'close': SKILL.md has no instructions after its frontmatter"
    # A render code: validating the authored catalog does not report it.
    assert validate_skill_catalog(SkillCatalog((skill("\n  \n"),), ())) == ()


def test_sst_rnd031_silent() -> None:
    _, diagnostics = build_skill_bundle(skill("Close the month.\n"))
    assert "SST-RND031" not in [item.code for item in diagnostics]
