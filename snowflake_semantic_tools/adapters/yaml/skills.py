"""Read skill folders and plugin manifests from disk into domain records."""

from __future__ import annotations

import posixpath
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import walk_refusal
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_strings,
    checked_text,
    optional_string,
    project_relative,
    refused,
    report_unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.skill import SKILL_FILE, Plugin, Skill, SkillCatalog, SkillFile

PLUGIN_FILES = ("plugin.yml", "plugin.yaml")
PLUGIN_KEYS = frozenset(("name", "description", "owner_team", "skills"))


def _published(path: Path, root: Path) -> bool:
    """Hidden entries and Python caches are authoring noise, never bundle content."""
    parts = path.relative_to(root).parts
    return not any(part.startswith(".") or part == "__pycache__" for part in parts) and path.suffix != ".pyc"


def load_skill_catalog(project_dir: Path, *, skills_dir: str, plugins_dir: str) -> SkillCatalog:
    """Every `SKILL.md` folder under `skills_dir`, at any grouping depth, plus plugin manifests.

    Nothing reached through a symbolic link is read: a linked file or folder under `skills_dir`,
    a linked plugin folder, or a linked plugin manifest is reported and left out. Each skill and
    plugin also reports the codes `_load_skill` and `_load_plugin` list.

    Diagnostics:
        SST-PRT009: once per symbolic link under `skills_dir` or `plugins_dir` that would be read.
        SST-PRS120: a folder holds files of its own but its `SKILL.md` sits in a subfolder.
    """
    diagnostics: list[Diagnostic] = []
    skills: list[Skill] = []
    root = project_dir / skills_dir
    if root.is_dir():
        # One walk reports every link under the root, so a skill folder's own walk need not.
        walked = [path for path in sorted(root.rglob("*")) if _published(path, root)]
        readable = [path for path in walked if not refused(project_dir, path, diagnostics)]
        folders = sorted(path.parent for path in readable if path.name == SKILL_FILE and path.is_file())
        nested = {
            folder: tuple(other for other in folders if other != folder and other.is_relative_to(folder))
            for folder in folders
        }
        inner = {child for children in nested.values() for child in children}
        diagnostics.extend(_misplaced_skill_files(project_dir, root, folders, readable))
        for folder in folders:
            if folder in inner:
                continue
            skills.append(_load_skill(project_dir, folder, nested[folder], diagnostics))
    plugins: list[Plugin] = []
    plugin_root = project_dir / plugins_dir
    if plugin_root.is_dir():
        for folder in sorted(path for path in plugin_root.iterdir() if _published(path, plugin_root)):
            if not folder.is_dir() or refused(
                project_dir, folder, diagnostics, subject=artifact_key("plugin", folder.name)
            ):
                continue
            plugin = _load_plugin(project_dir, folder, diagnostics)
            if plugin is not None:
                plugins.append(plugin)
    return SkillCatalog(tuple(skills), tuple(plugins), DiagnosticBag(diagnostics))


def _misplaced_skill_files(
    project_dir: Path, root: Path, folders: list[Path], readable: list[Path]
) -> tuple[Diagnostic, ...]:
    """Report a folder whose own files sit beside no `SKILL.md`, while its subfolder holds one.

    A grouping folder holds only folders; one that also holds files is a skill whose `SKILL.md`
    was put one level too deep, where Snowflake will not find it as that skill's.

    Diagnostics:
        SST-PRS120: once per such folder, naming the nested `SKILL.md`.
    """
    skill_folders = set(folders)
    found: list[Diagnostic] = []
    for folder in folders:
        parent = folder.parent
        if parent == root or parent in skill_folders:
            continue
        if any(path.parent == parent and path.is_file() for path in readable):
            subject = artifact_key("skill", parent.name)
            nested = project_relative(project_dir, folder / SKILL_FILE)
            found.append(D("SST-PRS120", origin=Origin(nested), subject=subject, artifact=subject, path=nested))
    return tuple(found)


def _load_skill(project_dir: Path, folder: Path, nested: tuple[Path, ...], diagnostics: list[Diagnostic]) -> Skill:
    """Read one skill folder: every published file in it, and the frontmatter of its `SKILL.md`.

    The skill is named after its folder. Each skill folder in `nested` is reported, and its files
    are left out of this skill; the caller does not load it as a skill either. The files are in
    path order, relative to the folder, `SKILL.md` among them.

    Raises:
        OSError: A file in the folder cannot be read.

    Diagnostics:
        SST-PRS119: once per skill folder nested inside this one.
        SST-LOD001: when `SKILL.md` is not UTF-8, or its frontmatter is not valid YAML.
        SST-LOD002: when the frontmatter is not a mapping.
        SST-PRS034: when the frontmatter has no `name` or no `description`, once for each.
    """
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
    # A linked file was reported by the catalog's walk of the skills root; here it is only left out.
    files = tuple(
        SkillFile(path.relative_to(folder).as_posix(), path.read_bytes())
        for path in sorted(folder.rglob("*"))
        if path.is_file()
        and _published(path, folder)
        and not any(path.is_relative_to(child) for child in nested)
        and walk_refusal(project_dir, path) is None
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
    """Split a `SKILL.md` into its frontmatter's `name` and `description`, and the body after it.

    The frontmatter runs from a first line that is exactly `---` to the next such line, and is
    read with `yaml.safe_load`. Without one, the whole text is the body.

    Returns:
        `(name, description, body)`: the name as written and the description stripped, each
        None when it is absent, blank or not a string, or the frontmatter cannot be read; the
        body is empty when the file is not UTF-8.

    Diagnostics:
        SST-LOD001: when the file is not UTF-8, at its first line, or the frontmatter is not valid
            YAML, at the offending line of the file.
        SST-LOD002: when the frontmatter is not a mapping.
        SST-PRS034: for `name` and for `description`, each that is missing or not a non-blank
            string; for both when there is no frontmatter.
    """
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
        value: Any = bounded_yaml.safe_load("".join(lines[1:closing]))
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
    """Read one plugin folder's manifest; None when it has no single manifest or it does not parse.

    The manifest is `plugin.yml` or `plugin.yaml`, and the plugin is named after its folder. A
    manifest that does not parse reports the codes `parse_yaml_bytes` lists, with the plugin as
    their subject. `skills` are kept as listed, in order and with any repeats.

    Raises:
        OSError: The manifest cannot be read.

    Diagnostics:
        SST-VAL801: when the folder holds no manifest or both spellings, or the manifest's
            `name` is not the folder's.
        SST-PRT009: when the manifest is a symbolic link; the plugin is left out.
        SST-PRS004: when the manifest holds a key SST does not read, at that key.
        SST-PRS003: when `description` or `owner_team` is not a string, or `skills` is not a list
            of strings.
        SST-PRS002: when there is no non-blank `description`.
    """
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
    if refused(project_dir, manifest, diagnostics, subject=subject):
        return None
    file = project_relative(project_dir, manifest)
    try:
        parsed = parse_yaml_bytes(manifest.read_bytes(), file)
    except ProjectError as exc:
        found = exc.diagnostics or (D("SST-LOD001", origin=Origin(file), file=file, line=1, col=1, detail=str(exc)),)
        diagnostics.extend(replace(item, subject=subject) for item in found)
        return None
    diagnostics.extend(replace(item, subject=subject) for item in parsed.diagnostics)
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
