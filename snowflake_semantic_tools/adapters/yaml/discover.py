"""Discover: find a project's semantic-model files, and decide which registered type owns each.

Discovery runs before anything is read. `discover_yaml` walks the semantic-models directory
for YAML files without opening one, and checks the configured directories of the registry's
artifact types against each other; `load_documents` then reads what it found. Once the files
are loaded, `assign_owners` gives each to the types whose root keys it holds -- a file's
location is a hint, its content the authority.

The walk does not descend into a symbolic link to a directory, as `Path.rglob` does not; one
that points back into its own tree is a loop, and is reported.
"""

from __future__ import annotations

import os
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_KEYS, configured_dir
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, Registry

YAML_SUFFIXES = frozenset((".yml", ".yaml"))
CANONICAL_SUFFIX = ".yml"
# How many directories deep below the semantic-models directory a file may sit.
MAX_DEPTH = 32


@dataclass(frozen=True, slots=True)
class DiscoveredFile:
    """One YAML file found under the semantic-models directory, before anything reads it.

    Attributes:
        path: Relative to the project directory, in POSIX form: how diagnostics name the file.
        abs_path: The file under the project directory as given, so relative when that is;
            `RawDocument.abs_path` is resolved.
        size: In bytes, when discovered.
        mtime_ns: The modification time when discovered, in nanoseconds since the epoch.
        hint_root: The first directory below the semantic-models directory on the file's path;
            None for a file directly in it.
    """

    path: str
    abs_path: Path
    size: int
    mtime_ns: int
    hint_root: str | None


@dataclass(frozen=True, slots=True)
class FileSet:
    """The YAML files one run discovers, in path order, before any of them is read.

    Attributes:
        roots: Each discovered directory by its role; `discover_yaml` records `semantic_models`,
            the directory as configured.
        diagnostics: What discovery found wrong, which `load_documents` carries into its result.
    """

    files: tuple[DiscoveredFile, ...]
    roots: Mapping[str, str]
    diagnostics: tuple[Diagnostic, ...] = ()


def registry_roots(config: Mapping[str, object], registry: Registry = SEMANTIC_REGISTRY) -> Mapping[str, str]:
    """Return each artifact type's directory, relative to the project, as `config` configures it.

    A type's directory is its `dir_key`'s, or that key's default; a type nested under another
    is `<parent>/*/<type>s`, inside each of the parent's artifact folders.
    """
    own = {
        artifact.name: configured_dir(config, artifact.dir_key, str(CONFIG_KEYS[f"project.{artifact.dir_key}"].default))
        for artifact in registry.artifacts.values()
    }
    return MappingProxyType(
        {
            name: f"{own[artifact.nested_under]}/*/{name}s" if artifact.nested_under else own[name]
            for name, artifact in registry.artifacts.items()
        }
    )


def discover_yaml(
    project_dir: Path,
    semantic_models_dir: str,
    registry: Registry = SEMANTIC_REGISTRY,
    *,
    config: Mapping[str, object] | None = None,
) -> FileSet:
    """Discover the YAML files under the semantic-models directory; no file content is opened here.

    The diagnostics are the directory claims first, then the walk's, in path order.

    Args:
        config: The parsed `sst_config.yml`, whose directories are checked against each other;
            None checks the defaults.

    Raises:
        ProjectError: SST-DIS001 or SST-DIS002, carried as its diagnostic: the directory is
            absent, or is not a directory.

    Diagnostics:
        SST-DIS007: two artifact types' directories are one directory, or one holds the other.
        SST-DIS003: the walk found no YAML file.
        SST-DIS004, SST-DIS005, SST-DIS006, SST-LOD200, SST-LOD020: as `_walk` reports them.
    """
    root = project_dir / semantic_models_dir
    if not root.exists():
        diagnostic = D("SST-DIS001", path=semantic_models_dir)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    if not root.is_dir():
        diagnostic = D("SST-DIS002", path=semantic_models_dir)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    claims = shared_directories(registry_roots(config or {}, registry), registry)
    found, problems = _walk(project_dir, root)
    if not found:
        problems.append(D("SST-DIS003", path=semantic_models_dir))
    return FileSet(tuple(found), MappingProxyType({"semantic_models": semantic_models_dir}), (*claims, *problems))


def shared_directories(roots: Mapping[str, str], registry: Registry = SEMANTIC_REGISTRY) -> tuple[Diagnostic, ...]:
    """Report each directory two artifact types claim: one directory, or one inside the other.

    A type nested under another claims its own subtree of the parent's, which is not a clash.
    Each clash is reported once, at the inner directory, in type order.
    """
    found: list[Diagnostic] = []
    names = list(roots)
    for index, first in enumerate(names):
        for second in names[index + 1 :]:
            if second in _nesting(first, registry) or first in _nesting(second, registry):
                continue
            inner = _inner(PurePosixPath(roots[first]), PurePosixPath(roots[second]))
            if inner is not None:
                found.append(D("SST-DIS007", path=str(inner), types=[first, second]))
    return tuple(found)


def _nesting(name: str, registry: Registry) -> frozenset[str]:
    """The types `name` nests under, transitively."""
    parents: set[str] = set()
    current = registry.artifacts[name].nested_under
    while current and current not in parents:
        parents.add(current)
        current = registry.artifacts[current].nested_under
    return frozenset(parents)


def _inner(first: PurePosixPath, second: PurePosixPath) -> PurePosixPath | None:
    """The directory of the two that lies inside the other, or either when equal; None for neither."""
    if first == second or first.is_relative_to(second):
        return first
    if second.is_relative_to(first):
        return second
    return None


def _walk(project_dir: Path, root: Path) -> tuple[list[DiscoveredFile], list[Diagnostic]]:
    """Walk `root` depth first, in name order, for the YAML files it holds.

    Diagnostics:
        SST-DIS004: a file cannot be read.
        SST-DIS005: a directory link points back into the tree, or a directory is deeper than
            `MAX_DEPTH`; it is not walked.
        SST-DIS006: a path equals an earlier one under case folding; the later is left out.
        SST-LOD200, SST-LOD020: as `_extension_notes` reports them, for each file found.
    """
    found: list[DiscoveredFile] = []
    problems: list[Diagnostic] = []
    folded: dict[str, str] = {}
    pending: list[tuple[Path, int]] = [(root, 0)]
    while pending:
        directory, depth = pending.pop()
        children: list[tuple[Path, int]] = []
        for entry in sorted(directory.iterdir()):
            relative = entry.relative_to(project_dir).as_posix()
            if entry.is_dir():
                if entry.is_symlink():
                    if _loops(entry, root):
                        problems.append(D("SST-DIS005", path=relative))
                elif depth + 1 > MAX_DEPTH:
                    problems.append(D("SST-DIS005", path=relative))
                else:
                    children.append((entry, depth + 1))
                continue
            if not entry.is_file() or entry.suffix.casefold() not in YAML_SUFFIXES:
                continue
            key = fold_key(relative)
            if key in folded:
                problems.append(D("SST-DIS006", a=folded[key], b=relative))
            elif not os.access(entry, os.R_OK):
                problems.append(D("SST-DIS004", path=relative))
            else:
                folded[key] = relative
                problems.extend(_extension_notes(relative, entry.suffix))
                found.append(_discovered(entry, relative, root))
        pending.extend(reversed(children))
    return sorted(found, key=lambda item: PurePosixPath(item.path).parts), problems


def _extension_notes(relative: str, suffix: str) -> tuple[Diagnostic, ...]:
    """Say when a file is read under an extension other than `.yml`.

    Diagnostics:
        SST-LOD200: the file uses `.yaml`, which SST accepts as it does `.yml`.
        SST-LOD020: the file uses another spelling SST tolerates, such as `.YML`.
    """
    if suffix == CANONICAL_SUFFIX:
        return ()
    if suffix == ".yaml":
        return (D("SST-LOD200", origin=Origin(relative), file=relative),)
    return (D("SST-LOD020", origin=Origin(relative), file=relative, found=suffix),)


def fold_key(path: str) -> str:
    """Return the form two paths share when a case-insensitive file system holds them as one."""
    return unicodedata.normalize("NFC", path).casefold()


def _loops(link: Path, root: Path) -> bool:
    """Report whether a directory link points at the tree's root or at a directory on its own path."""
    target = link.resolve()
    return target == root.resolve() or link.parent.resolve().is_relative_to(target)


def _discovered(path: Path, relative: str, root: Path) -> DiscoveredFile:
    stat = path.stat()
    below = path.relative_to(root).parts
    return DiscoveredFile(relative, path, stat.st_size, stat.st_mtime_ns, below[0] if len(below) > 1 else None)
