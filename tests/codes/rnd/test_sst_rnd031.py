"""SST-RND031: a skill gives no instructions after its frontmatter."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog


def catalog(body: str) -> SkillCatalog:
    content = f"---\nname: close\n---\n{body}".encode()
    skill = Skill(
        "close", "skills/close", "close", "Close.", body, (SkillFile("SKILL.md", content),), Origin("SKILL.md")
    )
    return SkillCatalog((skill,), ())


def test_sst_rnd031_fires() -> None:
    [diagnostic] = validate_skill_catalog(catalog("\n  \n"))
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-RND031", Severity.WARNING, "skill:close")
    assert diagnostic.message == "skill 'close': SKILL.md has no instructions after its frontmatter"


def test_sst_rnd031_silent() -> None:
    assert validate_skill_catalog(catalog("Close the month.\n")) == ()
