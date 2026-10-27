"""Map a live skill or plugin -- an extension version, or a stage folder -- to the project's files.

A SKILL version holds one skill folder under `skills/<name>/`; a PLUGIN version holds
`.cortex-plugin/plugin.json` and each member's folder the same way (`domain.render.skill_bundle`).
A skill is written as its folder, every file copied byte for byte, under the type's directory;
a plugin as its `plugin.yml`, which lists its members by `skill()`, plus each member the project
does not publish already. A skill read from a stage path is one skill folder there.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.importing.model import ImportedFile, ImportedObject
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.references import ProjectIndex
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

_SKILLS = "skills/"


@dataclass(frozen=True, slots=True)
class LiveBundle:
    """The files of one extension version, or of a stage folder, by path below its root.

    Attributes:
        name: The extension, or the stage a stage path reads.
        files: Each file's bytes, by its path below the version's root.
        comment: The extension's comment; None for a stage folder.
        manifest: A plugin's `.cortex-plugin/plugin.json`, parsed; empty for a skill.
        source: What was read, as a person names it: the extension, or the stage path.
    """

    name: QualifiedName
    files: Mapping[str, bytes]
    comment: str | None = None
    manifest: Mapping[str, object] | None = None
    source: str = ""


@dataclass(frozen=True, slots=True)
class BundlePlacement:
    """Where a bundle's files go: the type's directory, the folder below it, and the skills directory."""

    root: str
    destination: Destination
    skills_root: str


def skill_folders(files: Mapping[str, bytes]) -> tuple[str, ...]:
    """Return the skill folders a bundle holds under `skills/`, in path order."""
    return tuple(
        dict.fromkeys(path[len(_SKILLS) :].split("/", 1)[0] for path in sorted(files) if path.startswith(_SKILLS))
    )


def import_skill(bundle: LiveBundle, index: ProjectIndex, placement: BundlePlacement) -> ImportedObject:
    """Map a SKILL version, or a stage folder, to its skill folder: every file copied as it is.

    Diagnostics:
        SST-IMP007: the project publishes a skill of that name already.
        SST-IMP013: the version holds no skill folder, or more than one, or publishes under another
            extension name than the live one.
    """
    folders = skill_folders(bundle.files)
    rooted = not folders and "SKILL.md" in bundle.files
    if len(folders) != 1 and not rooted:
        name = bundle.name.name.value.casefold().replace("_", "-")
        detail = f"{bundle.source} holds {len(folders)} skill folders, not one; it was not imported"
        return _refused(
            "skill",
            bundle.name,
            name,
            D("SST-IMP013", subject=f"skill:{name}", artifact=f"skill:{name}", detail=detail),
        )
    name = folders[0] if folders else posixpath.basename(bundle.source.rstrip("/"))
    key = f"skill:{name}"
    if name.casefold() in index.skill_names:
        return _refused("skill", bundle.name, name, D("SST-IMP007", subject=key, artifact=key, value=bundle.source))
    diagnostics: list[Diagnostic] = []
    files = _skill_files(bundle.files, name, rooted, placement.skills_root, placement.destination)
    _named_as(bundle, name, key, diagnostics)
    return ImportedObject("skill", bundle.name, name, files, diagnostics=tuple(diagnostics))


def import_plugin(bundle: LiveBundle, index: ProjectIndex, placement: BundlePlacement) -> ImportedObject:
    """Map a PLUGIN version to its `plugin.yml` and each member skill the project does not publish.

    Diagnostics:
        SST-IMP007: the project publishes a plugin of that name already.
        SST-IMP013: the version publishes under another extension name than the live one.
    """
    manifest = bundle.manifest or {}
    name = str(manifest.get("name") or bundle.name.name.value.casefold().replace("_", "-"))
    key = f"plugin:{name}"
    if index.named("plugin", name) is not None:
        return _refused("plugin", bundle.name, name, D("SST-IMP007", subject=key, artifact=key, value=bundle.source))
    members = skill_folders(bundle.files)
    files: list[ImportedFile] = []
    for member in members:
        if member.casefold() not in index.skill_names:
            files.extend(_skill_files(bundle.files, member, False, placement.skills_root, placement.destination))
    document: dict[str, object] = {"name": name}
    description = manifest.get("description") or bundle.comment
    if description:
        document["description"] = str(description)
    author = manifest.get("author")
    if isinstance(author, Mapping) and author.get("name"):
        document["owner_team"] = str(author["name"])
    document["skills"] = ["{{ skill('" + member + "') }}" for member in members]
    folder = posixpath.join(placement.root, *placement.destination.folder, file_name(name))
    diagnostics: list[Diagnostic] = []
    _named_as(bundle, name, key, diagnostics)
    plugin_file = ImportedFile(posixpath.join(folder, "plugin.yml"), document)
    return ImportedObject("plugin", bundle.name, name, (plugin_file, *files), diagnostics=tuple(diagnostics))


def _skill_files(
    files: Mapping[str, bytes], name: str, rooted: bool, root: str, destination: Destination
) -> tuple[ImportedFile, ...]:
    prefix = "" if rooted else f"{_SKILLS}{name}/"
    folder = posixpath.join(root, *destination.folder, file_name(name))
    return tuple(
        ImportedFile(posixpath.join(folder, path[len(prefix) :]), data=data)
        for path, data in sorted(files.items())
        if path.startswith(prefix) and not path.endswith("/")
    )


def _named_as(bundle: LiveBundle, name: str, key: str, diagnostics: list[Diagnostic]) -> None:
    """Report an extension SST would publish under another name than the live one."""
    published = name.upper().replace("-", "_")
    from_extension = bundle.manifest is not None or not bundle.source.startswith("@")
    if from_extension and published != bundle.name.name.folded:
        detail = (
            f"it publishes as {published}, after its name, not as the live {bundle.name.name.folded}; "
            "agents that load the live extension keep loading it until they load the project's"
        )
        diagnostics.append(D("SST-IMP013", subject=key, artifact=key, detail=detail))


def _refused(artifact_type: str, name: QualifiedName, artifact: str, diagnostic: Diagnostic) -> ImportedObject:
    return ImportedObject(artifact_type, name, artifact, diagnostics=(diagnostic,))
