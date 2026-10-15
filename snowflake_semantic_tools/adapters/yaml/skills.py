"""Read skill folders and plugin manifests from disk into domain records."""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import walk_refusal
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_text,
    member_names,
    optional_string,
    project_relative,
    refused,
    report_unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.skill import SKILL_FILE, Plugin, Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

PLUGIN_FILES = ("plugin.yml", "plugin.yaml")
_NONE: Mapping[str, object] = MappingProxyType({})
PLUGIN_KEYS = frozenset(("name", "description", "owner_team", "skills", "config"))


def _published(path: Path, root: Path) -> bool:
    """Hidden entries and Python caches are authoring noise, never bundle content."""
    parts = path.relative_to(root).parts
    return not any(part.startswith(".") or part == "__pycache__" for part in parts) and path.suffix != ".pyc"


def load_skill_catalog(
    project_dir: Path, *, skills_dir: str, plugins_dir: str, config: Mapping[str, Any] | None = None
) -> SkillCatalog:
    """Every `SKILL.md` folder under `skills_dir`, at any grouping depth, plus plugin manifests.

    A plugin is a folder under `plugins_dir` that holds a manifest; a folder with neither a
    manifest nor a published subfolder is a plugin missing one, and any other folder only groups
    plugins. Nothing reached through a symbolic link is read: a linked file or folder under
    `skills_dir`, a linked plugin folder, or a linked plugin manifest is reported and left out.
    Each skill and plugin also reports the codes `_load_skill` and `_load_plugin` list.

    Args:
        config: The run's resolved configuration, whose `skills:` and `plugins:` folder routes
            are checked against the folders under each root; None checks no route. Each item
            records its folder and its own `config:`, which compile resolves over the routes.

    Diagnostics:
        SST-CFG041: a `skills:` or `plugins:` folder route names no folder under its root.
        SST-PRT009: once per symbolic link under `skills_dir` or `plugins_dir` that would be read.
        SST-PRS120: a folder holds files of its own but its `SKILL.md` sits in a subfolder.
    """
    diagnostics: list[Diagnostic] = list(routed_diagnostics(config, project_dir, "skill", "plugin")) if config else []
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
            skill = _load_skill(project_dir, folder, nested[folder], diagnostics)
            skills.append(replace(skill, folder=folder.relative_to(root).parts))
    plugins: list[Plugin] = []
    plugin_root = project_dir / plugins_dir
    for folder in _plugin_folders(project_dir, plugin_root, plugin_root, diagnostics):
        plugin = _load_plugin(project_dir, folder, diagnostics)
        if plugin is not None:
            plugins.append(replace(plugin, folder=folder.relative_to(plugin_root).parts))
    return SkillCatalog(tuple(skills), tuple(plugins), DiagnosticBag(diagnostics))


def _plugin_folders(project_dir: Path, directory: Path, root: Path, diagnostics: list[Diagnostic]) -> Iterator[Path]:
    """Yield each plugin folder below `directory`, in path order, descending into grouping folders.

    A folder that holds a manifest, or no published subfolder, is a plugin folder; the walk does
    not descend into one. A linked folder is reported and left out.

    Diagnostics:
        SST-PRT009: a folder that would be read is a symbolic link.
    """
    if not directory.is_dir():
        return
    for folder in sorted(path for path in directory.iterdir() if _published(path, root)):
        if not folder.is_dir() or refused(
            project_dir, folder, diagnostics, subject=artifact_key("plugin", folder.name)
        ):
            continue
        grouping = not any((folder / name).is_file() for name in PLUGIN_FILES) and any(
            child.is_dir() and _published(child, root) for child in folder.iterdir()
        )
        if grouping:
            yield from _plugin_folders(project_dir, folder, root, diagnostics)
        else:
            yield folder


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

    The frontmatter's `config:` is kept as the skill's own settings when it is a mapping.

    Raises:
        OSError: A file in the folder cannot be read.

    Diagnostics:
        SST-PRS119: once per skill folder nested inside this one.
        SST-LOD001: when `SKILL.md` is not UTF-8, or its frontmatter is not valid YAML.
        SST-LOD002: when the frontmatter is not a mapping.
        SST-PRS034: when the frontmatter has no `name` or no `description`, once for each.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports the frontmatter's `config:` block.
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
    frontmatter, body = _frontmatter(raw, skill_md, subject, diagnostics)
    item_config = frontmatter.get("config")
    diagnostics.extend(item_config_diagnostics("skills", item_config, subject=subject, origin_of=_at(skill_md)))
    return Skill(
        name=name,
        directory=directory,
        declared_name=_declared(frontmatter.get("name")),
        description=optional_string(frontmatter.get("description")),
        body=body,
        files=files,
        origin=Origin(skill_md, 1),
        config=_own_settings(item_config),
    )


def _own_settings(value: object) -> Mapping[str, object]:
    """An item's `config:` block as its own settings; empty when it is absent or not a mapping."""
    return MappingProxyType({str(key): item for key, item in value.items()}) if isinstance(value, Mapping) else _NONE


def _declared(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _at(file: str) -> Callable[[str], Origin]:
    """Place every key of a `SKILL.md` frontmatter's `config:` block at the file's first line."""

    def origin_of(key: str) -> Origin:
        del key
        return Origin(file, 1)

    return origin_of


def _frontmatter(raw: bytes, file: str, subject: str, diagnostics: list[Diagnostic]) -> tuple[dict[str, object], str]:
    """Split a `SKILL.md` into its frontmatter and the body after it.

    The frontmatter runs from a first line that is exactly `---` to the next such line, and is
    read with `yaml.safe_load`. Without one, the whole text is the body.

    Returns:
        `(keys, body)`: the frontmatter's keys as parsed, none when there is no frontmatter or it
        cannot be read; and the body, empty when the file is not UTF-8.

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
        return {}, ""
    lines = text.splitlines(keepends=True)
    closing = next(
        (index for index, line in enumerate(lines[1:], start=1) if line.rstrip("\r\n") == "---"),
        None,
    )
    if not lines or lines[0].rstrip("\r\n") != "---" or closing is None:
        for field in ("name", "description"):
            diagnostics.append(D("SST-PRS034", origin=Origin(file, 1), subject=subject, artifact=subject, field=field))
        return {}, text
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
        return {}, body
    if value is None:
        value = {}
    if not isinstance(value, dict):
        diagnostics.append(
            D("SST-LOD002", origin=Origin(file, 2), subject=subject, file=file, found=type(value).__name__)
        )
        return {}, body
    for field in ("name", "description"):
        found = value.get(field)
        if not isinstance(found, str) or not found.strip():
            diagnostics.append(D("SST-PRS034", origin=Origin(file, 1), subject=subject, artifact=subject, field=field))
    return {str(key): item for key, item in value.items()}, body


def _plugin_manifest(
    project_dir: Path, folder: Path, subject: str, diagnostics: list[Diagnostic]
) -> tuple[str, ParsedYaml] | None:
    """Parse a plugin folder's one manifest, with its path; None when there is not one that parses.

    Raises:
        OSError: The manifest cannot be read.

    Diagnostics:
        SST-VAL801: when the folder holds no manifest or both spellings.
        SST-PRT009: when the manifest is a symbolic link.
        The codes `parse_yaml_bytes` lists, with the plugin as their subject.
    """
    manifests = [folder / candidate for candidate in PLUGIN_FILES if (folder / candidate).is_file()]
    if len(manifests) != 1:
        detail = "has both plugin.yml and plugin.yaml" if manifests else "has no plugin.yml"
        origin = Origin(project_relative(project_dir, folder))
        diagnostics.append(
            D("SST-VAL801", origin=origin, subject=subject, artifact=subject, detail=f"plugin folder {detail}")
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
    return file, parsed


def _load_plugin(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> Plugin | None:
    """Read one plugin folder's manifest; None when it has no single manifest or it does not parse.

    The manifest is `plugin.yml` or `plugin.yaml`, and the plugin is named after its folder. A
    manifest that does not parse reports the codes `parse_yaml_bytes` lists, with the plugin as
    their subject. `skills` are kept as listed, in order and with any repeats, and a `config:`
    mapping as the plugin's own settings.

    Raises:
        OSError: The manifest cannot be read.

    Diagnostics:
        SST-VAL801: when the folder holds no manifest or both spellings, or the manifest's
            `name` is not the folder's.
        SST-PRT009: when the manifest is a symbolic link; the plugin is left out.
        SST-PRS004: when the manifest holds a key SST does not read, at that key.
        SST-PRS003: when `description` or `owner_team` is not a string, or `skills` is not a list
            of strings.
        Every code `member_names` lists, for a `skills` entry that is not one
            `{{ skill('<name>') }}` call.
        SST-PRS002: when there is no non-blank `description`.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports the manifest's `config:` block.
    """
    name = folder.name
    subject = artifact_key("plugin", name)
    directory = project_relative(project_dir, folder)
    found = _plugin_manifest(project_dir, folder, subject, diagnostics)
    if found is None:
        return None
    file, parsed = found
    tree = dict(parsed.tree)

    def origin(key: str) -> Origin:
        position = parsed.line_index.get(tuple(key.split(".")))
        return Origin(file, position.line, position.col) if position is not None else Origin(file)

    report_unknown_keys(tree, PLUGIN_KEYS, diagnostics, artifact=subject, origin=origin, subject=subject)
    diagnostics.extend(
        item_config_diagnostics(
            "plugins",
            tree.get("config"),
            subject=subject,
            origin_of=lambda key: origin(f"config.{key}" if key else "config"),
        )
    )
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
    members = member_names(
        tree.get("skills"),
        "plugin.skills",
        diagnostics,
        field="skills",
        artifact=subject,
        origin=origin("skills"),
        subject=subject,
    )
    return Plugin(
        name=name,
        directory=directory,
        manifest_file=file,
        description=text_fields["description"],
        owner_team=text_fields["owner_team"],
        members=members,
        origin=Origin(file, 1),
        config=_own_settings(tree.get("config")),
    )
