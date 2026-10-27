"""Load semantic-layer YAML exactly once per compiler run, from the files `discover` found.

This is the one place a single-item file is told from a list: a mapping under a type's singular
root (`metric:`) is read as a one-item list under its list root (`snowflake_metrics:`), with every
node position moved to match, so everything downstream reads both shapes the same way.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.discover import DiscoveredFile, FileSet
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.parse.records import singular_roots

# Each singular root key, by the list root its one-item file reads as.
SINGULAR_ROOTS: Mapping[str, str] = singular_roots(SEMANTIC_REGISTRY)


@dataclass(frozen=True, slots=True)
class TemplateSource:
    """One `{{ ... }}` template exactly as written, and where it starts in its file.

    Attributes:
        raw: The template's source text, braces included.
        line: 1-based.
        col: 1-based, the column of the opening `{{`.
    """

    raw: str
    line: int
    col: int


@dataclass(frozen=True, slots=True)
class ParsedYaml:
    """One YAML file as `parse_yaml_bytes` parses it: its tree, node positions, and templates.

    Attributes:
        tree: The root mapping, read-only, with string keys at the top level; nested values are
            as YAML typed them, and every template is restored, as written, in every string.
        line_index: Where the value at each node path starts: the path is the mapping keys and
            list indexes from the root, `()` for the root itself. Empty for a document that is
            only `null`.
        templates: Each template, by the placeholder that stood in for it while YAML parsed.
        diagnostics: What the parse found that does not stop the file loading, such as CRLF
            line endings or a folded scalar, in the order found.
    """

    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    templates: Mapping[str, TemplateSource]
    diagnostics: tuple[Diagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class RawDocument:
    """One semantic-model file, read and parsed once, with what its parse recorded.

    Attributes:
        path: Relative to the project directory, in POSIX form: how diagnostics name the file.
        abs_path: The file, resolved.
        checksum: The SHA-256 of the file's bytes, as lowercase hex.
        tree, line_index, templates: As `ParsedYaml` holds them, except that a singular root's
            mapping is a one-item list under its list root, and its positions are keyed so.
        root_keys: The tree's top-level keys, in file order, each singular root as its list root.
        hint_root: As `discover.DiscoveredFile.hint_root`.
        single_item_roots: The list roots the file wrote in their singular form.
    """

    path: str
    abs_path: Path
    checksum: str
    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    templates: Mapping[str, TemplateSource]
    root_keys: tuple[str, ...]
    hint_root: str | None
    single_item_roots: frozenset[str] = frozenset()

    def position(self, path: NodePath) -> SourcePosition | None:
        """Return where the value at a node path starts; None when the file records no such path."""
        return self.line_index.get(path)


@dataclass(frozen=True, slots=True)
class RawDocuments:
    """Every discovered file's parse, and the files that did not parse.

    Attributes:
        documents: The files that parsed, in discovery order.
        by_path: The same documents, by `RawDocument.path`.
        failed: The paths, as diagnostics name them, of the files that could not be parsed; none
            of them is in `documents`.
        diagnostics: The discovery diagnostics, then each file's in discovery order: why a file
            failed, or what a loaded file's formatting was found to be.
    """

    documents: tuple[RawDocument, ...]
    by_path: Mapping[str, RawDocument]
    failed: tuple[str, ...]
    diagnostics: tuple[Diagnostic, ...]

    def under(self, directory: Path, root_key: str) -> tuple[RawDocument, ...]:
        """Return the documents at any depth below `directory` whose tree has the top-level `root_key`.

        `directory` is resolved before the comparison and `root_key` must match exactly; the
        documents keep their discovery order.
        """
        resolved = directory.resolve()
        return tuple(
            document
            for document in self.documents
            if document.abs_path.is_relative_to(resolved) and root_key in document.root_keys
        )


class LoadCache:
    """The parses of files already read in this run, by resolved path and the SHA-256 of their bytes.

    A file is served from the cache only while its bytes are unchanged, so an edit between two
    reads is always parsed again. Whether a read was served is progress, not a finding about the
    project: it is noted only when `note_hits` asks, as `--verbose` does, so a run's diagnostics
    never depend on how often SST read a file.
    """

    def __init__(self, *, note_hits: bool = False) -> None:
        self._parsed: dict[tuple[Path, str], ParsedYaml] = {}
        self.note_hits = note_hits

    def get(self, path: Path, checksum: str) -> ParsedYaml | None:
        """Return the parse of `path` with these bytes, or None when it was not read with them."""
        return self._parsed.get((path.resolve(), checksum))

    def put(self, path: Path, checksum: str, parsed: ParsedYaml) -> None:
        """Remember the parse of `path` with these bytes."""
        self._parsed[(path.resolve(), checksum)] = parsed


def _parsed(
    discovered: DiscoveredFile,
    raw_bytes: bytes,
    checksum: str,
    parse_document: Callable[[bytes, str], ParsedYaml],
    cache: LoadCache | None,
) -> tuple[ParsedYaml, tuple[Diagnostic, ...]]:
    """Parse one file's bytes, or serve the parse the cache holds for them.

    Raises:
        ProjectError: As `parse_document` raises; a file that does not parse is never cached.

    Diagnostics:
        SST-LOD201: the parse came from a cache that notes its hits; the parse's own findings
            follow it.
    """
    if cache is None:
        parsed = parse_document(raw_bytes, discovered.path)
        return parsed, parsed.diagnostics
    cached = cache.get(discovered.abs_path, checksum)
    if cached is None:
        parsed = parse_document(raw_bytes, discovered.path)
        cache.put(discovered.abs_path, checksum, parsed)
        return parsed, parsed.diagnostics
    if not cache.note_hits:
        return cached, cached.diagnostics
    note = D("SST-LOD201", origin=Origin(discovered.path), file=discovered.path)
    return cached, (note, *cached.diagnostics)


@dataclass(frozen=True, slots=True)
class _ItemShape:
    """A parsed tree with each singular root read as a one-item list, and what that found."""

    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    single_item_roots: frozenset[str]
    diagnostics: tuple[Diagnostic, ...]


def _as_lists(path: str, parsed: ParsedYaml, roots: Mapping[str, str] = SINGULAR_ROOTS) -> _ItemShape:
    """Read each singular root's mapping as a one-item list under its list root, keeping key order.

    A position under `(<singular>, ...)` moves to `(<list root>, 0, ...)`, and the list root's own
    position is the mapping's, so a check places the node exactly where the file writes it. A
    singular root that cannot be read so is dropped from the tree, with its positions.

    Diagnostics:
        SST-PRS003: a file holds both a type's singular root and its list root, which reads the
            list alone; or a singular root does not hold a mapping.
    """
    found = [key for key in parsed.tree if key in roots]
    if not found:
        return _ItemShape(parsed.tree, parsed.line_index, frozenset(), ())
    tree: dict[str, Any] = {}
    moved: dict[str, str] = {}
    diagnostics: list[Diagnostic] = []
    for key, value in parsed.tree.items():
        listed = roots.get(key)
        if listed is None:
            tree[key] = value
            continue
        origin = _key_origin(path, parsed, key)
        if listed in parsed.tree:
            expected = f"one of {key}: and {listed}:"
            diagnostics.append(
                D("SST-PRS003", origin=origin, artifact=path, field="root", expected=expected, found="both")
            )
        elif not isinstance(value, Mapping):
            found_type = type(value).__name__
            diagnostics.append(
                D("SST-PRS003", origin=origin, artifact=path, field=key, expected="a mapping", found=found_type)
            )
        else:
            tree[listed] = [value]
            moved[key] = listed
    return _ItemShape(
        MappingProxyType(tree),
        MappingProxyType(_moved_positions(parsed.line_index, moved, frozenset(found))),
        frozenset(moved.values()),
        tuple(diagnostics),
    )


def _key_origin(path: str, parsed: ParsedYaml, key: str) -> Origin:
    position = parsed.line_index.get((key,))
    return Origin(path, position.line if position else None, position.col if position else None)


def _moved_positions(
    line_index: Mapping[NodePath, SourcePosition], moved: Mapping[str, str], singular: frozenset[str]
) -> dict[NodePath, SourcePosition]:
    """Key each position under a moved singular root as the one-item list's; drop the rest of `singular`."""
    positions: dict[NodePath, SourcePosition] = {}
    for node_path, position in line_index.items():
        head = node_path[0] if node_path else None
        if head not in singular:
            positions[node_path] = position
        elif head in moved:
            listed = moved[str(head)]
            if len(node_path) == 1:
                positions[(listed,)] = position
            positions[(listed, 0, *node_path[1:])] = position
    return positions


def load_documents(
    files: FileSet,
    parse_document: Callable[[bytes, str], ParsedYaml],
    cache: LoadCache | None = None,
) -> RawDocuments:
    """Read, hash, and parse each discovered file once, or take its parse from `cache`.

    Each file's singular roots are read as one-item lists, as `_as_lists` does.

    Diagnostics:
        SST-LOD201: as `_parsed` reports it, for a file the cache served.
        SST-PRS003: as `_as_lists` reports it.
    """
    documents: list[RawDocument] = []
    failed: list[str] = []
    diagnostics: list[Diagnostic] = list(files.diagnostics)
    for discovered in files.files:
        try:
            raw_bytes = discovered.abs_path.read_bytes()
            parsed, found = _parsed(discovered, raw_bytes, sha256(raw_bytes).hexdigest(), parse_document, cache)
        except ProjectError as exc:
            failed.append(discovered.path)
            diagnostics.extend(exc.diagnostics)
            continue
        shape = _as_lists(discovered.path, parsed)
        diagnostics.extend((*found, *shape.diagnostics))
        document = RawDocument(
            path=discovered.path,
            abs_path=discovered.abs_path.resolve(),
            checksum=sha256(raw_bytes).hexdigest(),
            tree=shape.tree,
            line_index=shape.line_index,
            templates=parsed.templates,
            root_keys=tuple(str(key) for key in shape.tree),
            hint_root=discovered.hint_root,
            single_item_roots=shape.single_item_roots,
        )
        documents.append(document)
    by_path = MappingProxyType({document.path: document for document in documents})
    return RawDocuments(tuple(documents), by_path, tuple(failed), tuple(diagnostics))
