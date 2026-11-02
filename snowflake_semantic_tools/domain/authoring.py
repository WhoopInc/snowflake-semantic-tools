"""The files a new artifact or semantic member starts as, and where each goes.

A new item is one file, or one folder, at the place SST discovers its kind: a semantic view or a
member is a one-item file in the semantic-model directory, a tool member one in the tools
directory, and an agent, a CoWork or Cortex Code profile, a plugin or a skill is a folder named
for it holding its manifest. Each starts with its name and nothing else, so what it still needs
is what `validate` reports about it. Every path and root key comes from the artifact registry
and `file_name`, as discovery reads them, so a new file is found where it was put.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.model.authored_schema.locations import DIRECTORY_DEFAULTS
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.skill.model import SKILL_FILE

# Types each a folder named for the item, holding a manifest of this stem: `agents/<name>/agent.yml`.
_FOLDER_MANIFESTS: Mapping[str, str] = {
    "agent": "agent",
    "cowork_profile": "cowork_profile",
    "coco_profile": "coco_profile",
    "plugin": "plugin",
}
# Types whose routes go one folder deep, so an item there sits at the root of its directory.
_ROOT_ONLY = frozenset(("coco_profile",))

# Every kind `new_files` starts: each artifact type but an eval, which `eval_files` starts beside
# its subject, then each member authored in files.
NEW_KINDS: tuple[str, ...] = (
    *(name for name in SEMANTIC_REGISTRY.artifacts if name != "eval"),
    *(name for name, member in SEMANTIC_REGISTRY.members.items() if member.singular_root),
)


@dataclass(frozen=True, slots=True)
class NewFile:
    """One file a new item starts as.

    Attributes:
        path: Where it goes, relative to the project, with `/` separators.
        document: Its YAML document; for a `SKILL.md`, the front matter.
        body: The Markdown after the front matter; None for a YAML file.
    """

    path: str
    document: Mapping[str, object]
    body: str | None = None


def new_files(kind: str, name: str, folder: tuple[str, ...], directories: Mapping[str, str]) -> tuple[NewFile, ...]:
    """Return the files a new item of `kind` named `name` starts as, in `folder` below its directory.

    Args:
        kind: One of `NEW_KINDS`, such as `agent` or `metric`.
        folder: The folder below the kind's directory, one segment per directory; its routes
            decide where the item publishes. A Cortex Code profile takes none.
        directories: Each `project.*_dir` as the configuration sets it; one absent takes its
            default.

    Raises:
        ValueError: `kind` is not one of `NEW_KINDS`; `name` is empty or not one path segment;
            a folder segment is not one; or a Cortex Code profile is given a folder.
    """
    if kind not in NEW_KINDS:
        raise ValueError(f"{kind!r} is not a kind SST starts files for")
    _check_segments((name, *folder))
    if kind in _ROOT_ONLY and folder:
        raise ValueError(f"a {kind} sits at the root of its directory, not in a folder")
    if kind in SEMANTIC_REGISTRY.members:
        root = _directory("semantic_models_dir", directories)
        singular = SEMANTIC_REGISTRY.members[kind].singular_root
        return (NewFile(_join(root, folder, f"{file_name(name)}.yml"), {str(singular): {"name": name}}),)
    artifact = SEMANTIC_REGISTRY.artifacts[kind]
    root = _directory(artifact.dir_key, directories)
    if artifact.singular_root is not None:
        return (NewFile(_join(root, folder, f"{file_name(name)}.yml"), {artifact.singular_root: {"name": name}}),)
    own = (*folder, file_name(name))
    if kind == "skill":
        return (NewFile(_join(root, own, SKILL_FILE), {"name": name, "description": ""}, body=f"# {name}\n"),)
    return (NewFile(_join(root, own, f"{_FOLDER_MANIFESTS[kind]}.yml"), {"name": name}),)


def eval_files(subject_folder: str, name: str) -> tuple[NewFile, NewFile]:
    """Return the dataset and config a new eval starts as, in `evals/` of its subject's folder.

    Args:
        subject_folder: The folder holding the agent's or CoWork profile's manifest, relative
            to the project.
        name: What the dataset and config are named after, the subject's name.

    Raises:
        ValueError: `name` is empty or not one path segment.
    """
    _check_segments((name,))
    evals = posixpath.join(subject_folder, "evals")
    dataset = NewFile(posixpath.join(evals, "dataset.yml"), {"name": eval_names(name)[0], "questions": []})
    config = NewFile(posixpath.join(evals, "config.yml"), {"name": eval_names(name)[1]})
    return dataset, config


def eval_names(name: str) -> tuple[str, str]:
    """Return the names a new eval's dataset and config take, after its subject's `name`."""
    return f"{name}_questions", f"{name}_eval"


def _directory(key: str, directories: Mapping[str, str]) -> str:
    return posixpath.normpath(directories.get(key) or DIRECTORY_DEFAULTS[key])


def _join(root: str, folder: tuple[str, ...], leaf: str) -> str:
    return posixpath.join(root, *folder, leaf)


def _check_segments(segments: tuple[str, ...]) -> None:
    """Refuse a name or folder segment that is empty, hidden, or not one path segment as written."""
    for segment in segments:
        if not segment or segment != segment.strip() or segment in (".", "..") or segment.startswith("."):
            raise ValueError(f"{segment!r} is not a name a file or folder can take")
        if "/" in segment or "\\" in segment or "\x00" in segment:
            raise ValueError(f"{segment!r} is not one path segment")
