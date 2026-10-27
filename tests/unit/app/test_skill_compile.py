"""Skill and plugin compile: a plugin is blocked by its members' errors and by its own bundle."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.render.skill_bundle import SCAN_MAX_FILES
from tests.helpers.compile_builders import plugin

# The byte split and publication-surface notes every compiled extension reports.
NOTES = frozenset(("SST-VAL816", "SST-VAL831"))

CHANNEL = CatalogChannel("DB", "S", QualifiedName.parse("DB.S.BUNDLES"))


def skill(name: str, references: int = 0) -> Skill:
    paths = [f"reference/page-{index}.md" for index in range(references)]
    body = "".join(f"See {path}.\n" for path in paths) or "Body.\n"
    files = (
        SkillFile("SKILL.md", f"---\nname: {name}\ndescription: D.\n---\n{body}".encode()),
        *(SkillFile(path, b"page\n") for path in paths),
    )
    return Skill(name, f"skills/{name}", name, "D.", body, files, Origin(f"skills/{name}/SKILL.md"))


def test_a_plugin_over_the_scan_limits_is_blocked_while_its_members_publish() -> None:
    # Each member is within the file-count limit alone; bundled together they are not.
    half = SCAN_MAX_FILES // 2 + 1
    catalog = SkillCatalog((skill("first", half), skill("second", half)), (plugin("kit", "first", "second"),))

    result = CompileSkills(catalog, CHANNEL).run_result()

    assert [item.artifact_key for item in result.compiled] == ["skill:first", "skill:second"]
    assert [(item.code, item.subject) for item in result.diagnostics if item.code not in NOTES] == [
        ("SST-VAL834", "plugin:kit")
    ]


def test_a_healthy_plugin_carries_its_members_sources_and_scripts() -> None:
    catalog = SkillCatalog((skill("first"), skill("second")), (plugin("kit", "first", "second", "first"),))

    result = CompileSkills(catalog, CHANNEL).run_result()

    kit = next(item for item in result.compiled if item.artifact_key == "plugin:kit")
    assert isinstance(kit, CompiledExtension)
    assert kit.source_files == ("plugins/kit/plugin.yml", "skills/first/SKILL.md", "skills/second/SKILL.md")
    assert kit.contained_keys == ("skill:first", "skill:second")
    assert kit.has_scripts is False


def test_a_name_that_cannot_name_an_extension_is_reported_instead_of_raising() -> None:
    # The naming rule allows a leading digit; an unquoted Snowflake name does not.
    catalog = SkillCatalog((skill("9lives"), skill("first")), (plugin("9kit", "first"),))

    result = CompileSkills(catalog, CHANNEL).run_result()

    assert [item.artifact_key for item in result.compiled] == ["skill:first"]
    assert [(item.code, item.subject, item.message) for item in result.diagnostics if item.code not in NOTES] == [
        (
            "SST-VAL801",
            "skill:9lives",
            "'9lives': folder name '9lives' publishes as 9LIVES, which must start with a letter to name an extension",
        ),
        (
            "SST-VAL801",
            "plugin:9kit",
            "'9kit': plugin name '9kit' publishes as 9KIT, which must start with a letter to name an extension",
        ),
    ]
