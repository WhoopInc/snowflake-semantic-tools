"""Discover and load semantic-layer YAML exactly once per compiler run."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, TypeAlias

from ...domain.model.diagnostic import Diagnostic
from ...domain.model.registry import SEMANTIC_REGISTRY, Registry
from ..project import ProjectError

YAML_SUFFIXES = frozenset((".yml", ".yaml"))
NodePath: TypeAlias = tuple[str | int, ...]


@dataclass(frozen=True, slots=True)
class SourcePosition:
    line: int
    col: int


@dataclass(frozen=True, slots=True)
class TemplateSource:
    raw: str
    line: int
    col: int


@dataclass(frozen=True, slots=True)
class ParsedYaml:
    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    templates: Mapping[str, TemplateSource]


@dataclass(frozen=True, slots=True)
class DiscoveredFile:
    path: str
    abs_path: Path
    size: int
    mtime_ns: int
    hint_root: str | None


@dataclass(frozen=True, slots=True)
class FileSet:
    files: tuple[DiscoveredFile, ...]
    roots: Mapping[str, str]
    diagnostics: tuple[Diagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class RawDocument:
    path: str
    abs_path: Path
    checksum: str
    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    templates: Mapping[str, TemplateSource]
    root_keys: tuple[str, ...]
    hint_root: str | None

    def position(self, path: NodePath) -> SourcePosition | None:
        return self.line_index.get(path)


@dataclass(frozen=True, slots=True)
class RawDocuments:
    documents: tuple[RawDocument, ...]
    by_path: Mapping[str, RawDocument]
    failed: tuple[str, ...]
    diagnostics: tuple[Diagnostic, ...]

    def under(self, directory: Path, root_key: str) -> tuple[RawDocument, ...]:
        resolved = directory.resolve()
        return tuple(
            document
            for document in self.documents
            if document.abs_path.is_relative_to(resolved) and root_key in document.root_keys
        )


def discover_yaml(
    project_dir: Path,
    semantic_models_dir: str,
    registry: Registry = SEMANTIC_REGISTRY,
) -> FileSet:
    """Discover paths only; no file content is opened here."""
    root = project_dir / semantic_models_dir
    del registry
    if not root.is_dir():
        raise ProjectError(f"no semantic model directory under {root}")
    found: list[DiscoveredFile] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.casefold() not in YAML_SUFFIXES:
            continue
        stat = path.stat()
        relative = path.relative_to(project_dir).as_posix()
        relative_to_root = path.relative_to(root)
        hint_root = relative_to_root.parts[0] if len(relative_to_root.parts) > 1 else None
        hint_root = hint_root
        found.append(
            DiscoveredFile(
                path=relative,
                abs_path=path,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                hint_root=hint_root,
            )
        )
    return FileSet(tuple(found), MappingProxyType({"semantic_models": semantic_models_dir}))


def load_documents(
    files: FileSet,
    parse_document: Callable[[bytes, str], ParsedYaml],
) -> RawDocuments:
    """Read, hash, and parse each discovered file once."""
    documents: list[RawDocument] = []
    failed: list[str] = []
    diagnostics: list[Diagnostic] = list(files.diagnostics)
    for discovered in files.files:
        try:
            raw_bytes = discovered.abs_path.read_bytes()
            parsed = parse_document(raw_bytes, discovered.path)
        except ProjectError as exc:
            failed.append(discovered.path)
            diagnostics.extend(exc.diagnostics)
            continue
        document = RawDocument(
            path=discovered.path,
            abs_path=discovered.abs_path.resolve(),
            checksum=sha256(raw_bytes).hexdigest(),
            tree=parsed.tree,
            line_index=parsed.line_index,
            templates=parsed.templates,
            root_keys=tuple(str(key) for key in parsed.tree),
            hint_root=discovered.hint_root,
        )
        documents.append(document)
    by_path = MappingProxyType({document.path: document for document in documents})
    return RawDocuments(tuple(documents), by_path, tuple(failed), tuple(diagnostics))
