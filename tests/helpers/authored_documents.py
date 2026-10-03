"""Semantic-model documents built in memory, for the tests that call the semantic checks directly.

Each satisfies `AuthoredDocument` with a tree and the node positions a test names, so a check
reads it as it reads a parsed file.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition


@dataclass(frozen=True)
class Document:
    """One semantic-model file: its tree, and where each node path a test names starts."""

    path: str
    tree: Mapping[str, Any]
    line_index: Mapping[NodePath, SourcePosition] = field(default_factory=dict)
    hint_root: str | None = None

    @property
    def root_keys(self) -> tuple[str, ...]:
        return tuple(self.tree)

    def position(self, path: NodePath) -> SourcePosition | None:
        return self.line_index.get(path)


@dataclass(frozen=True)
class Documents:
    """Every file, in discovery order."""

    documents: Sequence[Document]


def documents(*files: Document) -> Documents:
    """The files given, in order."""
    return Documents(files)


def document(
    path: str, tree: Mapping[str, Any], *lines: tuple[NodePath, int], hint_root: str | None = None
) -> Document:
    """A file whose node paths start on the lines given, each at column 1."""
    return Document(path, tree, {node: SourcePosition(line, 1) for node, line in lines}, hint_root)


ORIGIN = Origin("semantic_models/metrics.yml", 2, 3)
