"""Read skill folders and plugin manifests from disk into domain records."""

from __future__ import annotations

import posixpath
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml

from ...domain.model.artifact_key import artifact_key
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ...domain.model.skill import SKILL_FILE, Plugin, Skill, SkillCatalog, SkillFile
from ..project import ProjectError
from .fields import checked_strings, checked_text, optional_string, project_relative, report_unknown_keys
from .parse import parse_yaml_bytes

PLUGIN_FILES = ("plugin.yml", "plugin.yaml")
PLUGIN_KEYS = frozenset(("name", "description", "owner_team", "skills"))


def _published(path: Path, root: Path) -> bool:
    """Hidden entries and Python caches are authoring noise, never bundle content."""
    parts = path.relative_to(root).parts
    return not any(part.startswith(".") or part == "__pycache__" for part in parts) and path.suffix != ".pyc"


def load_skill_catalog(project_dir: Path, *, skills_dir: str, plugins_dir: str) -> SkillCatalog:
    """Every `SKILL.md` folder under `skills_dir`, at any grouping depth, plus plugin manifests."""
    diagnostics: list[Diagnostic] = []
    skills: list[Skill] = []
    root = project_dir / skills_dir
    if root.is_dir():
        folders = sorted(path.parent for path in root.rglob(SKILL_FILE) if path.is_file() and _published(path, root))
        nested = {
            folder: tuple(other for other in folders if other != folder and other.is_relative_to(folder))
            for folder in folders
        }
        inner = {child for children in nested.values() for child in children}
        for folder in folders:
            if folder in inner:
                continue
            skills.append(_load_skill(project_dir, folder, nested[folder], diagnostics))
    plugins: list[Plugin] = []
    plugin_root = project_dir / plugins_dir
    if plugin_root.is_dir():
        for folder in sorted(path for path in plugin_root.iterdir() if path.is_dir() and _published(path, plugin_root)):
            plugin = _load_plugin(project_dir, folder, diagnostics)
            if plugin is not None:
                plugins.append(plugin)
    return SkillCatalog(tuple(skills), tuple(plugins), DiagnosticBag(diagnostics))


def _load_skill(project_dir: Path, folder: Path, nested: tuple[Path, ...], diagnostics: list[Diagnostic]) -> Skill:
    name = folder.name
    directory = project_relative(project_dir, folder)
    skill_md = posixpath.join(directory, SKILL_FILE)
    subject = artifact_key("skill", name)
    for child in nested:
        diagnostics.append(
            D(
                "SST-PRS119",
                origin=Origin(skill_md),
                subject=subject,
                artifact=subject,
                path=project_relative(project_dir, child / SKILL_FILE),
            )
        )
    files = tuple(
        SkillFile(path.relative_to(folder).as_posix(), path.read_bytes())
        for path in sorted(folder.rglob("*"))
        if path.is_file() and _published(path, folder) and not any(path.is_relative_to(child) for child in nested)
    )
    raw = next(item.content for item in files if item.path == SKILL_FILE)
    declared_name, description, body = _frontmatter(raw, skill_md, subject, diagnostics)
    return Skill(
        name=name,
        directory=directory,
        declared_name=declared_name,
        description=description,
        body=body,
        files=files,
        origin=Origin(skill_md, 1),
    )


def _frontmatter(
    raw: bytes, file: str, subject: str, diagnostics: list[Diagnostic]
) -> tuple[str | None, str | None, str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        diagnostics.append(
            D("SST-LOD001", origin=Origin(file), subject=subject, file=file, line=1, col=1, detail=f"not UTF-8: {exc}")
        )
        return None, None, ""
    lines = text.splitlines(keepends=True)
    closing = next(
        (index for index, line in enumerate(lines[1:], start=1) if line.rstrip("\r\n") == "---"),
        None,
    )
    if not lines or lines[0].rstrip("\r\n") != "---" or closing is None:
        for field in ("name", "description"):
            diagnostics.append(D("SST-PRS034", origin=Origin(file, 1), subject=subject, artifact=subject, field=field))
        return None, None, text
    body = "".join(lines[closing + 1 :])
    try:
        value: Any = yaml.safe_load("".join(lines[1:closing]))
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        line = mark.line + 2 if mark is not None else 2
        col = mark.column + 1 if mark is not None else 1
        detail = str(getattr(exc, "problem", exc))
        diagnostics.append(
            D(
                "SST-LOD001",
                origin=Origin(file, line, col),
                subject=subject,
                file=file,
                line=line,
                col=col,
                detail=detail,
            )
        )
        return None, None, body
    if value is None:
        value = {}
    if not isinstance(value, dict):
        diagnostics.append(
            D("SST-LOD002", origin=Origin(file, 2), subject=subject, file=file, found=type(value).__name__)
        )
        return None, None, body
    declared = value.get("name")
    description = value.get("description")
    for field, found in (("name", declared), ("description", description)):
        if not isinstance(found, str) or not found.strip():
            diagnostics.append(D("SST-PRS034", origin=Origin(file, 1), subject=subject, artifact=subject, field=field))
    return (
        declared if isinstance(declared, str) and declared.strip() else None,
        optional_string(description),
        body,
    )


def _load_plugin(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> Plugin | None:
    name = folder.name
    subject = artifact_key("plugin", name)
    manifests = [folder / candidate for candidate in PLUGIN_FILES if (folder / candidate).is_file()]
    directory = project_relative(project_dir, folder)
    if len(manifests) != 1:
        detail = "has both plugin.yml and plugin.yaml" if manifests else "has no plugin.yml"
        diagnostics.append(
            D(
                "SST-VAL801",
                origin=Origin(directory),
                subject=subject,
                artifact=subject,
                detail=f"plugin folder {detail}",
            )
        )
        return None
    manifest = manifests[0]
    file = project_relative(project_dir, manifest)
    try:
        parsed = parse_yaml_bytes(manifest.read_bytes(), file)
    except ProjectError as exc:
        found = exc.diagnostics or (D("SST-LOD001", origin=Origin(file), file=file, line=1, col=1, detail=str(exc)),)
        diagnostics.extend(replace(item, subject=subject) for item in found)
        return None
    tree = dict(parsed.tree)

    def origin(key: str) -> Origin:
        position = parsed.line_index.get((key,))
        return Origin(file, position.line, position.col) if position is not None else Origin(file)

    report_unknown_keys(tree, PLUGIN_KEYS, diagnostics, artifact=subject, origin=origin, subject=subject)
    declared = tree.get("name")
    if declared is not None and declared != name:
        diagnostics.append(
            D(
                "SST-VAL801",
                origin=origin("name"),
                subject=subject,
                artifact=subject,
                detail=f"manifest name '{declared}' does not match the folder name",
            )
        )
    text_fields: dict[str, str | None] = {}
    for key in ("description", "owner_team"):
        text_fields[key] = checked_text(
            tree.get(key), diagnostics, field=key, artifact=subject, origin=origin(key), subject=subject
        )
    if text_fields["description"] is None:
        diagnostics.append(
            D("SST-PRS002", origin=origin("description"), subject=subject, artifact=subject, field="description")
        )
    members = checked_strings(
        tree.get("skills"),
        diagnostics,
        field="skills",
        artifact=subject,
        origin=origin("skills"),
        subject=subject,
        expected="a list of skill names",
    )
    return Plugin(
        name=name,
        directory=directory,
        manifest_file=file,
        description=text_fields["description"],
        owner_team=text_fields["owner_team"],
        members=members,
        origin=Origin(file, 1),
    )
