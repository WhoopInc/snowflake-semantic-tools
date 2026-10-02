"""Build the exact file set an extension version publishes, and hold it to Snowflake's scan limits.

A SKILL-type version is one skill flattened under `skills/<name>/`; a PLUGIN-type version is
`.cortex-plugin/plugin.json` plus each member flattened the same way. Entries are listed in
path order, so a bundle's digest, and with it the version alias, depends on content alone.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.skill.model import (
    SKILL_FILE,
    BundleEntry,
    Plugin,
    Skill,
    SkillBundle,
    SkillFile,
)
from snowflake_semantic_tools.domain.render.skill_flatten import flatten_skill
from snowflake_semantic_tools.domain.validate.shared import Emitter

PLUGIN_MANIFEST = ".cortex-plugin/plugin.json"
# Snowflake scans an extension version only within these limits.
SCAN_MAX_FILES = 50
SCAN_MAX_FILE_BYTES = 2 * 1024 * 1024
SCAN_MAX_TOTAL_BYTES = 10 * 1024 * 1024
# Soft budgets: SKILL.md is read on every orchestration turn, the bundle on demand.
SKILL_MD_BUDGET_BYTES = 25 * 1024
BUNDLE_BUDGET_BYTES = 1024 * 1024


def build_skill_bundle(skill: Skill) -> tuple[SkillBundle | None, DiagnosticBag]:
    """Build the flattened `skills/<name>/...` bundle a SKILL-type version is built from.

    Returns:
        The bundle, or None when the skill has an error or no files; and every diagnostic,
        flattening's first.

    Diagnostics:
        SST-VAL812: SKILL.md is over its size budget.
        SST-RND032: SKILL.md is within its size budget as authored and over it once flattened.
        SST-RND030: the flattened files hold no SKILL.md for the version to open with.
        SST-VAL834: the bundle breaks a scan limit on its file count or file or total size.
        SST-VAL811: the bundle is over its size budget, and within the scan limit on total size.
    """
    files, renames, diagnostics = flatten_skill(skill)
    found: list[Diagnostic] = list(diagnostics)
    skill_md = next((item for item in skill.files if item.path == SKILL_FILE), None)
    found.extend(
        _rendered_skill_md(skill, files, authored_over=skill_md is not None and skill_md.size > SKILL_MD_BUDGET_BYTES)
    )
    if skill_md is not None and skill_md.size > SKILL_MD_BUDGET_BYTES:
        found.append(
            D(
                "SST-VAL812",
                origin=skill.origin,
                subject=skill.key,
                artifact=skill.name,
                size=skill_md.size,
                expected=SKILL_MD_BUDGET_BYTES,
            )
        )
    entries = tuple(
        sorted(
            (BundleEntry(f"skills/{skill.name}/{item.path}", item.content) for item in files),
            key=lambda entry: entry.path,
        )
    )
    found.extend(_limit_diagnostics(skill.key, skill.name, skill.origin, entries))
    bag = DiagnosticBag(found)
    if bag.has_errors or not entries:
        return None, bag
    return SkillBundle("SKILL", skill.name, entries, renames), bag


def _rendered_skill_md(skill: Skill, files: tuple[SkillFile, ...], *, authored_over: bool) -> tuple[Diagnostic, ...]:
    """Check the flattened SKILL.md: present whenever anything is, and within budget if it was authored so."""
    rendered = next((item for item in files if item.path == SKILL_FILE), None)
    if rendered is None:
        if not files:
            return ()
        return (D("SST-RND030", origin=skill.origin, subject=skill.key, artifact=skill.name, path=SKILL_FILE),)
    if authored_over or rendered.size <= SKILL_MD_BUDGET_BYTES:
        return ()
    return (
        D(
            "SST-RND032",
            origin=skill.origin,
            subject=skill.key,
            artifact=skill.name,
            size=rendered.size,
            expected=SKILL_MD_BUDGET_BYTES,
        ),
    )


def plugin_manifest_json(plugin: Plugin) -> str:
    """Render the `.cortex-plugin/plugin.json` a plugin bundle opens with, as canonical JSON."""
    document: dict[str, object] = {"name": plugin.name, "skills": "./skills/"}
    if plugin.description:
        document["description"] = plugin.description
    if plugin.owner_team:
        document["author"] = {"name": plugin.owner_team}
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def build_plugin_bundle(
    plugin: Plugin,
    members: Mapping[str, Skill],
) -> tuple[SkillBundle | None, DiagnosticBag]:
    """Build `.cortex-plugin/plugin.json` plus each member flattened under `skills/<member>/`.

    A member missing from `members` is skipped here; validation reports it. A member listed
    twice is bundled once.

    Returns:
        The bundle, or None when any diagnostic is an error; and every diagnostic.

    Diagnostics:
        SST-VAL836: a member has errors, so it cannot be bundled.
        SST-RND030: no member was bundled, so the manifest's `./skills/` names nothing.
        SST-VAL834: the bundle breaks a scan limit on its file count or file or total size.
        SST-VAL811: the bundle is over its size budget, and within the scan limit on total size.
    """
    diagnostics: list[Diagnostic] = []
    entries: list[BundleEntry] = [BundleEntry(PLUGIN_MANIFEST, plugin_manifest_json(plugin).encode("utf-8"))]
    renames: list[tuple[str, str]] = []
    for name in dict.fromkeys(plugin.members):
        skill = members.get(name)
        if skill is None:
            continue
        files, member_renames, member_diagnostics = flatten_skill(skill)
        if member_diagnostics.has_errors:
            diagnostics.append(
                D("SST-VAL836", origin=plugin.origin, subject=plugin.key, artifact=plugin.name, name=name)
            )
            continue
        entries.extend(BundleEntry(f"skills/{name}/{item.path}", item.content) for item in files)
        renames.extend((f"{name}/{authored}", f"{name}/{published}") for authored, published in member_renames)
    ordered = tuple(sorted(entries, key=lambda entry: entry.path))
    if not any(entry.path.startswith("skills/") for entry in ordered) and not diagnostics:
        diagnostics.append(
            D("SST-RND030", origin=plugin.origin, subject=plugin.key, artifact=plugin.name, path="./skills/")
        )
    diagnostics.extend(_limit_diagnostics(plugin.key, plugin.name, plugin.origin, ordered))
    bag = DiagnosticBag(diagnostics)
    if bag.has_errors:
        return None, bag
    return SkillBundle("PLUGIN", plugin.name, ordered, tuple(renames)), bag


def _limit_diagnostics(
    subject: str, name: str, origin: Origin, entries: tuple[BundleEntry, ...]
) -> tuple[Diagnostic, ...]:
    """Hold a bundle to the scan limits, then to the soft size budget.

    The budget is reported only when the total is within its scan limit, which already says more.
    """
    emit = Emitter(subject=subject, origin=origin, artifact=name)
    if len(entries) > SCAN_MAX_FILES:
        emit("SST-VAL834", detail=f"{len(entries)} files, over the limit of {SCAN_MAX_FILES}")
    for entry in entries:
        if entry.size > SCAN_MAX_FILE_BYTES:
            emit(
                "SST-VAL834",
                detail=f"'{entry.path}' is {entry.size} bytes, over the per-file limit of {SCAN_MAX_FILE_BYTES}",
            )
    total = sum(entry.size for entry in entries)
    if total > SCAN_MAX_TOTAL_BYTES:
        emit("SST-VAL834", detail=f"{total} bytes in total, over the limit of {SCAN_MAX_TOTAL_BYTES}")
    elif total > BUNDLE_BUDGET_BYTES:
        emit("SST-VAL811", size=total, expected=BUNDLE_BUDGET_BYTES)
    return emit.diagnostics
