"""New artifacts and members as project files, and the edits that remove one; nothing is written.

`new_files` returns what a new item's files hold, at the place SST discovers its kind, in the
project's `format:` layout; `new_eval` adds an eval beside an agent or CoWork profile and names
it in the subject's manifest. `removal` returns the edits that take an artifact out of the
project: its own folder, or its file, or its entry in a file of several. A caller hands any of
them to `ProjectSession.with_overlay` to see the project as it would be, and writes them only
when it saves; what still refers to a removed artifact is what that overlay's `validate` reports.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.roundtrip import insert_key, load_editable, mapping_root, ordered_mapping
from snowflake_semantic_tools.adapters.yaml.emit import emit_document
from snowflake_semantic_tools.adapters.yaml.styles import YamlStyle, configured_style
from snowflake_semantic_tools.app.compile import CompiledArtifact
from snowflake_semantic_tools.domain.authoring import NewFile, eval_files, eval_names, new_files
from snowflake_semantic_tools.domain.model import authored_schema
from snowflake_semantic_tools.domain.model.authored_schema.locations import DIRECTORY_DEFAULTS
from snowflake_semantic_tools.domain.model.config_schema import configured_dir, route_root
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY

# The manifest of each type an eval may evaluate, and the order its authored keys are written in.
_SUBJECTS: Mapping[str, tuple[str, tuple[str, ...]]] = {
    "agent": ("agent", tuple(field.name for field in authored_schema.AGENT.node.fields)),
    "cowork_profile": ("cowork_profile", tuple(field.name for field in authored_schema.COWORK_PROFILE.node.fields)),
}
# Types whose artifact is its own folder, so removing it removes the folder.
_FOLDER_TYPES = frozenset(("agent", "cowork_profile", "coco_profile", "plugin", "skill"))


def scaffold(
    project_dir: Path, config: Mapping[str, object], kind: str, name: str, folder: tuple[str, ...]
) -> dict[str, bytes]:
    """Return the files a new item starts as, by project path, as `domain.authoring.new_files` places them.

    Raises:
        ValueError: as `new_files` raises.
        FileExistsError: a file it would start already exists.
    """
    directories = {key: configured_dir(config, key, default) for key, default in DIRECTORY_DEFAULTS.items()}
    return _written(project_dir, new_files(kind, name, folder, directories), configured_style(config))


def scaffold_eval(project_dir: Path, config: Mapping[str, object], subject: CompiledArtifact) -> dict[str, bytes]:
    """Return a new eval's dataset and config beside `subject`, and its manifest naming them.

    Raises:
        ValueError: `subject` is no agent or CoWork profile, or its manifest names an eval already.
        FileExistsError: its `evals/dataset.yml` or `evals/config.yml` exists already.
    """
    if subject.artifact_type not in _SUBJECTS:
        raise ValueError(f"an eval evaluates an agent or a CoWork profile, not a {subject.artifact_type}")
    stem, order = _SUBJECTS[subject.artifact_type]
    manifest = _manifest(subject, stem)
    document = load_editable((project_dir / manifest).read_text(encoding="utf-8"), manifest)
    root = mapping_root(document)
    if root is None or "evals" in root:
        raise ValueError(f"{manifest} names an eval already")
    files = _written(project_dir, eval_files(posixpath.dirname(manifest), subject.name), configured_style(config))
    dataset, config_name = eval_names(subject.name)
    named = ordered_mapping(
        (("dataset", f"{{{{ eval_dataset('{dataset}') }}}}"), ("config", f"{{{{ eval_config('{config_name}') }}}}"))
    )
    insert_key(root, "evals", named, order)
    files[manifest] = document.dump().encode("utf-8")
    return files


def removal(
    project_dir: Path, config: Mapping[str, object], item: CompiledArtifact, compiled: tuple[CompiledArtifact, ...]
) -> dict[str, bytes | None]:
    """Return the edits that take `item` out of the project, by project path; None deletes a file.

    An artifact that is its own folder -- an agent, a profile, a plugin, a skill -- goes with every
    file in it, its evals among them, except a folder nested in it that holds another artifact of
    its type, such as a skill inside a skill. A semantic view or tool member goes with its file,
    or, when its file declares others too, with its entry. An eval goes with its dataset and
    config, and its subject's manifest stops naming it.

    Raises:
        ValueError: `item` is an eval whose subject this compile does not hold.
    """
    if item.artifact_type in _FOLDER_TYPES:
        return _folder_removal(project_dir, config, item, compiled)
    if item.artifact_type == "eval":
        return _eval_removal(project_dir, item, compiled)
    return _entry_removal(project_dir, item)


def _folder_removal(
    project_dir: Path, config: Mapping[str, object], item: CompiledArtifact, compiled: tuple[CompiledArtifact, ...]
) -> dict[str, bytes | None]:
    root = route_root(config, ARTIFACT_REGISTRY.artifacts[item.artifact_type])
    own = posixpath.join(root, *item.route_folder)
    kept = tuple(
        posixpath.join(root, *other.route_folder)
        for other in compiled
        if other.artifact_type == item.artifact_type
        and other.artifact_key != item.artifact_key
        and other.route_folder[: len(item.route_folder)] == item.route_folder
    )
    edits: dict[str, bytes | None] = {}
    for path in sorted((project_dir / own).rglob("*")):
        relative = path.relative_to(project_dir).as_posix()
        if path.is_file() and not any(relative.startswith(f"{nested}/") for nested in kept):
            edits[relative] = None
    return edits


def _eval_removal(
    project_dir: Path, item: CompiledArtifact, compiled: tuple[CompiledArtifact, ...]
) -> dict[str, bytes | None]:
    """Remove an eval: its dataset and config, and its subject's `evals:`.

    Its subject is the agent or CoWork profile of its name in the same folder.
    """
    subject = next(
        (
            other
            for other in compiled
            if other.artifact_type in _SUBJECTS and other.name == item.name and other.route_folder == item.route_folder
        ),
        None,
    )
    if subject is None:
        raise ValueError(f"the subject of {item.artifact_key} is not compiled")
    manifest = _manifest(subject, _SUBJECTS[subject.artifact_type][0])
    edits: dict[str, bytes | None] = {path: None for path in item.source_files if path != manifest}
    document = load_editable((project_dir / manifest).read_text(encoding="utf-8"), manifest)
    root = mapping_root(document)
    if root is not None and "evals" in root:
        del root["evals"]
        edits[manifest] = document.dump().encode("utf-8")
    return edits


def _entry_removal(project_dir: Path, item: CompiledArtifact) -> dict[str, bytes | None]:
    """Remove a semantic view or tool member: its file, or its entry in a file declaring several."""
    artifact = ARTIFACT_REGISTRY.artifacts[item.artifact_type]
    declaring = _declaring_file(project_dir, item, artifact.singular_root, artifact.root_key)
    document = load_editable((project_dir / declaring).read_text(encoding="utf-8"), declaring)
    root = mapping_root(document)
    if root is None:
        return {declaring: None}
    single = root.get(artifact.singular_root)
    if isinstance(single, Mapping) and _named(single, item.name):
        del root[artifact.singular_root]
    entries = root.get(artifact.root_key)
    if isinstance(entries, list):
        entries[:] = [entry for entry in entries if not (isinstance(entry, Mapping) and _named(entry, item.name))]
        if not entries:
            del root[artifact.root_key]
    if not root:
        return {declaring: None}
    return {declaring: document.dump().encode("utf-8")}


def _declaring_file(project_dir: Path, item: CompiledArtifact, singular: str | None, plural: str | None) -> str:
    """The first of the item's source files whose document declares it under the type's roots."""
    for path in item.source_files:
        if not path.endswith((".yml", ".yaml")) or not (project_dir / path).is_file():
            continue
        root = load_editable((project_dir / path).read_text(encoding="utf-8"), path).root
        if not isinstance(root, Mapping):
            continue
        single = root.get(singular)
        entries = root.get(plural)
        if (isinstance(single, Mapping) and _named(single, item.name)) or (
            isinstance(entries, list)
            and any(isinstance(entry, Mapping) and _named(entry, item.name) for entry in entries)
        ):
            return path
    raise ValueError(f"no file of {item.artifact_key} declares it")


def _named(entry: Mapping[object, object], name: str) -> bool:
    return str(entry.get("name", "")).casefold() == name.casefold()


def _manifest(subject: CompiledArtifact, stem: str) -> str:
    """The subject's manifest: the source file named `<stem>.yml` or `<stem>.yaml`."""
    for path in subject.source_files:
        if posixpath.basename(path) in (f"{stem}.yml", f"{stem}.yaml"):
            return path
    raise ValueError(f"{subject.artifact_key} has no {stem}.yml")


def _written(project_dir: Path, files: tuple[NewFile, ...], style: YamlStyle) -> dict[str, bytes]:
    written: dict[str, bytes] = {}
    for item in files:
        if (project_dir / item.path).exists():
            raise FileExistsError(item.path)
        text = emit_document(item.document, item.path, style=style)
        if item.body is not None:
            text = f"---\n{text}---\n\n{item.body}"
        written[item.path] = text.encode("utf-8")
    return written
