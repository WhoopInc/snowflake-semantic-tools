"""Load semantic-layer YAML exactly once per compiler run, from the files `discover` found."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeAlias

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.discover import FileSet
from snowflake_semantic_tools.domain.diagnostics import Diagnostic

NodePath: TypeAlias = tuple[str | int, ...]


@dataclass(frozen=True, slots=True)
class SourcePosition:
    """Where a YAML node starts in its file, as a 1-based line and a 1-based column."""

    line: int
    col: int


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
            list indexes from the root, `()` for the root itself. A key a merge (`<<`) brings in
            is indexed under `<<`. Empty for a document that is only `null`.
        templates: Each template, by the placeholder that stood in for it while YAML parsed.
    """

    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    templates: Mapping[str, TemplateSource]


@dataclass(frozen=True, slots=True)
class RawDocument:
    """One semantic-model file, read and parsed once, with what its parse recorded.

    Attributes:
        path: Relative to the project directory, in POSIX form: how diagnostics name the file.
        abs_path: The file, resolved.
        checksum: The SHA-256 of the file's bytes, as lowercase hex.
        tree, line_index, templates: As `ParsedYaml` holds them.
        root_keys: The tree's top-level keys, in file order.
        hint_root: As `discover.DiscoveredFile.hint_root`.
    """

    path: str
    abs_path: Path
    checksum: str
    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition]
    templates: Mapping[str, TemplateSource]
    root_keys: tuple[str, ...]
    hint_root: str | None

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
        diagnostics: The discovery diagnostics, then each failed file's, in discovery order.
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
