"""Check each semantic-model file as text: folded scalars, and whether it is canonically formatted.

Both rules read the file's bytes rather than its parsed tree, since neither a scalar's style
nor a file's whitespace survives the parse.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.adapters.yaml.documents import NodePath, RawDocument, RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.checks.authored_keys import AUTHORED_KEYS
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _node_root
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key

# A block scalar header that folds: `key: >`, `key: >-`, `- >+2`, with an optional comment.
_FOLDED = re.compile(r"^\s*(?:-\s+)*(?:[^\s#][^#]*?:\s+)?>[-+0-9]*\s*(?:#.*)?$")


def _file_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """Report, file by file in discovery order, each folded scalar and then the file's formatting.

    Diagnostics:
        SST-VAL008: a multi-line string of a semantic-model node uses `>` or `>-`.
        SST-VAL009: the file is not canonically formatted.
    """
    diagnostics: list[Diagnostic] = []
    for document in documents.documents:
        try:
            text = document.abs_path.read_bytes().decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        diagnostics.extend(_folded_scalars(document, text))
        if _formatting_problem(text) is not None:
            diagnostics.append(D("SST-VAL009", origin=Origin(document.path), path=document.path))
    return tuple(diagnostics)


def _formatting_problem(text: str) -> str | None:
    """Name the first way `text` is not canonically formatted; None when it is.

    Canonical is LF line endings, no tab in a line's indentation, no trailing whitespace, and
    exactly one newline at the end of a non-empty file.
    """
    if "\r" in text:
        return "a carriage return"
    for line in text.split("\n"):
        if line != line.rstrip():
            return "trailing whitespace"
        if "\t" in line[: len(line) - len(line.lstrip())]:
            return "a tab in its indentation"
    if text and (not text.endswith("\n") or text.endswith("\n\n")):
        return "no single final newline"
    return None


def _folded_scalars(document: RawDocument, text: str) -> Iterator[Diagnostic]:
    """Report each folded block scalar that is a field of a semantic-model node."""
    roots = {_node_root(node_type): node_type for node_type in AUTHORED_KEYS}
    by_line = _paths_by_line(document.line_index)
    for number, line in enumerate(text.split("\n"), start=1):
        if not _FOLDED.match(line):
            continue
        path = by_line.get(number)
        if path is None or len(path) < 3 or path[0] not in roots or not isinstance(path[1], int):
            continue
        node_type = roots[str(path[0])]
        nodes = document.tree.get(str(path[0]))
        node = nodes[path[1]] if isinstance(nodes, list) and path[1] < len(nodes) else None
        name = str(node.get("name") or path[1]) if isinstance(node, dict) else str(path[1])
        yield D(
            "SST-VAL008",
            origin=Origin(document.path, number),
            subject=artifact_key(node_type, name),
            type=node_type,
            name=name,
            field=_field(path[2:]),
        )


def _paths_by_line(line_index: Mapping[NodePath, object]) -> dict[int, NodePath]:
    """The deepest node path that starts on each line: the value a block scalar header opens."""
    found: dict[int, NodePath] = {}
    for path, position in line_index.items():
        line = getattr(position, "line", None)
        if isinstance(line, int) and len(path) >= len(found.get(line, ())):
            found[line] = path
    return found


def _field(parts: tuple[object, ...]) -> str:
    """A node path below its node, as a diagnostic names a field: `window.order_by[0].ref`."""
    label = ""
    for part in parts:
        label += f"[{part}]" if isinstance(part, int) else (f".{part}" if label else str(part))
    return label
