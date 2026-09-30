"""Check a skill catalog: folder and plugin names, staged file names, uniqueness, plugin members.

Skills and plugins publish into one extension namespace. Skills are checked in directory
order, then plugins in directory order, and each name is claimed by the first artifact to
reach it, so a plugin that collides with a skill is always the one reported.
"""

from __future__ import annotations

from collections.abc import Container, Sequence

from ..diagnostic import Diagnostic, DiagnosticBag
from ..stage_path import ALLOWED_DESCRIPTION, unsafe_segment
from ..validation import SKILL_NAMES, Emitter, duplicates
from .model import Plugin, Skill, SkillCatalog


def validate_skill_catalog(catalog: SkillCatalog) -> DiagnosticBag:
    """Check folder, naming, uniqueness, and plugin-membership rules across the catalog.

    The loader's diagnostics come first, then each skill's, then each plugin's.

    Diagnostics:
        SST-VAL801: a skill folder or plugin name is not kebab-case, or the frontmatter disagrees.
        SST-RND031: a skill has no instructions after its frontmatter.
        SST-VAL857: a skill file has a name a stage rejects.
        SST-VAL832: a skill or plugin takes an extension name an earlier one already took.
        SST-PRS002: a plugin lists no skills.
        SST-VAL837: a plugin lists a member more than once; reported once per member, by name.
        SST-VAL835: a plugin member is not a skill in this project; reported per listing.
    """
    skills = sorted(catalog.skills, key=lambda item: item.directory)
    plugins = sorted(catalog.plugins, key=lambda item: item.directory)
    taken = _extension_collisions((*skills, *plugins))
    names = {skill.name for skill in catalog.skills}
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    for index, skill in enumerate(skills):
        diagnostics.extend(_skill_rules(skill, taken.get(index)))
    for index, plugin in enumerate(plugins, start=len(skills)):
        diagnostics.extend(_plugin_rules(plugin, taken.get(index), names))
    return DiagnosticBag(diagnostics)


def _extension_collisions(artifacts: Sequence[Skill | Plugin]) -> dict[int, str]:
    """Map each repeat's position to the directory of the artifact that took its extension name first.

    Positions, not the records, key the result, so two equal records stay distinct.
    """
    repeats = duplicates(enumerate(artifacts), lambda pair: pair[1].extension_name)
    return {index: first.directory for (_, first), (index, _) in repeats}


def _skill_rules(skill: Skill, other: str | None) -> tuple[Diagnostic, ...]:
    """Check one skill folder; `other` is the directory that took its extension name first."""
    emit = Emitter(subject=skill.key, origin=skill.origin, artifact=skill.name)
    problem = SKILL_NAMES.problem(skill.name)
    if problem is not None:
        emit("SST-VAL801", detail=f"folder name '{skill.name}' is not {problem}")
    if skill.declared_name is not None and skill.declared_name != skill.name:
        emit("SST-VAL801", detail=f"frontmatter name '{skill.declared_name}' does not match the folder name")
    if not skill.body.strip():
        emit("SST-RND031")
    for file in sorted(skill.files, key=lambda item: item.path):
        unsafe = unsafe_segment(file.path)
        if unsafe is not None:
            emit(
                "SST-VAL857",
                origin=skill.origin_of(file.path),
                artifact=skill.key,
                value=file.path,
                found=unsafe,
                expected=ALLOWED_DESCRIPTION,
            )
    if other is not None:
        emit("SST-VAL832", artifact=skill.directory, other=other)
    return emit.diagnostics


def _plugin_rules(plugin: Plugin, other: str | None, skills: Container[str]) -> tuple[Diagnostic, ...]:
    """Check one plugin manifest; `other` is the directory that took its extension name first."""
    emit = Emitter(subject=plugin.key, origin=plugin.origin, artifact=plugin.name)
    problem = SKILL_NAMES.problem(plugin.name)
    if problem is not None:
        emit("SST-VAL801", detail=f"plugin name '{plugin.name}' is not {problem}")
    if other is not None:
        emit("SST-VAL832", artifact=plugin.directory, other=other)
    if not plugin.members:
        emit("SST-PRS002", artifact=plugin.key, field="skills")
    for member in sorted({repeat for _, repeat in duplicates(plugin.members, lambda name: name)}):
        emit("SST-VAL837", name=member)
    for member in plugin.members:
        if member not in skills:
            emit("SST-VAL835", name=member)
    return emit.diagnostics
