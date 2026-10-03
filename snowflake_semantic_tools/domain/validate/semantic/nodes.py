"""Walk the nodes of semantic-model documents: where each one sits, and the root key that holds it."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.authored import AuthoredDocument, AuthoredDocuments, NodePath
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY


def list_of(value: object) -> list[Any]:
    """Return `value` when it is a list, else an empty one."""
    return value if isinstance(value, list) else []


def node_root(node_type: str) -> str:
    """Return the top-level key a semantic view or a member type is authored under."""
    if node_type == "semantic_view":
        root_key = SEMANTIC_REGISTRY.artifacts[node_type].root_key
        assert root_key is not None
        return root_key
    return member_root(node_type)


def member_root(name: str) -> str:
    """Return the top-level key a member type is authored under."""
    return SEMANTIC_REGISTRY.members[name].root_key


def node_origin(document: AuthoredDocument, root_key: str, index: int, field: str | None = None) -> Origin:
    """Return where the node at `root_key[index]`, or its `field`, starts in its document."""
    path: NodePath = (root_key, index) if field is None else (root_key, index, field)
    position = document.position(path)
    return Origin(
        file=document.path,
        line=position.line if position is not None else None,
        col=position.col if position is not None else None,
    )


def load_nodes(documents: AuthoredDocuments, root_key: str) -> tuple[tuple[AuthoredDocument, int, dict[str, Any]], ...]:
    """Return every mapping node under `root_key`, in document order, with its document and index.

    A node's `description` is read on one line and stripped; an entry that is not a mapping is
    left out.
    """
    loaded: list[tuple[AuthoredDocument, int, dict[str, Any]]] = []
    for document in documents.documents:
        if root_key not in document.root_keys:
            continue
        for index, node in enumerate(document.tree.get(root_key) or []):
            if isinstance(node, dict):
                loaded.append((document, index, normalise_node_strings(node)))
    return tuple(loaded)


def normalise_node_strings(node: dict[str, Any]) -> dict[str, Any]:
    """Return a node with string keys and its `description` on one line, stripped."""
    return {
        str(key): (
            " ".join(value.splitlines()).strip() if str(key) == "description" and isinstance(value, str) else value
        )
        for key, value in node.items()
    }
