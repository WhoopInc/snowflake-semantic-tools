"""Report the keys read only to say what becomes of them: honoured 0.3 spellings and inert keys.

A 0.3 spelling SST still honours is named with its 1.0 key; a key that changes nothing in the
DDL is named so it does not pass review looking as though it does.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocument, RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _node_root
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key

# 0.3 spellings SST still reads, by node type: each maps to its 1.0 key. Written alone, the
# old key is honoured and reported; written beside the 1.0 key, it is refused.
DEPRECATED_KEYS: Mapping[str, Mapping[str, str]] = MappingProxyType(
    {
        "custom_instruction": MappingProxyType(
            {"sql_generation": "ai_sql_generation", "question_categorization": "ai_question_categorization"}
        ),
        "metric": MappingProxyType({"visibility": "access_modifier"}),
    }
)
# Keys SST reads only to report that the renderer emits nothing for them. A filter's
# `synonyms:` is not read: it is SST-VAL406's, as an unread key.
INERT_KEYS: Mapping[str, tuple[str, ...]] = MappingProxyType({"relationship": ("relationship_type", "join_type")})
# The values 0.3 wrote under `visibility`, as the access modifiers they mean.
VISIBILITY: Mapping[str, str] = MappingProxyType({"public": "public_access", "private": "private_access"})


def honoured(node: Mapping[str, Any], key: str, node_type: str) -> object:
    """Return `key`'s value, or its 0.3 spelling's when only that is written; None when neither is."""
    if key in node:
        return node[key]
    old = next((old for old, new in DEPRECATED_KEYS.get(node_type, {}).items() if new == key), None)
    value = node.get(old) if old is not None else None
    if node_type == "metric" and isinstance(value, str):
        return VISIBILITY.get(value.strip().casefold(), value)
    return value


def _deprecated_key_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """Report every honoured 0.3 spelling and every inert key, node by node in document order.

    Diagnostics:
        SST-VAL012: a custom instruction writes a 0.3 key spelling, which is honoured.
        SST-VAL122: a metric writes `visibility`, which is honoured as `access_modifier`.
        SST-PRS020: a 0.3 spelling is written beside its 1.0 key, so it is not honoured.
        SST-VAL211: a relationship declares `relationship_type` or `join_type`.
    """
    node_types = sorted({*DEPRECATED_KEYS, *INERT_KEYS})
    return tuple(
        diagnostic
        for document in documents.documents
        for node_type in node_types
        for index, node in _nodes(document, node_type)
        for diagnostic in _node_diagnostics(document, node_type, index, node)
    )


def _nodes(document: RawDocument, node_type: str) -> Iterator[tuple[int, Mapping[str, Any]]]:
    root_key = _node_root(node_type)
    nodes = document.tree.get(root_key) if root_key in document.root_keys else None
    for index, node in enumerate(nodes if isinstance(nodes, list) else ()):
        if isinstance(node, dict) and node.get("name"):
            yield index, node


def _node_diagnostics(
    document: RawDocument, node_type: str, index: int, node: Mapping[str, Any]
) -> Iterator[Diagnostic]:
    """Report one node's honoured 0.3 spellings, then its inert keys, each in table order."""
    name = str(node["name"])
    subject = artifact_key(node_type, name)

    def origin(key: str) -> Origin:
        position = document.position((_node_root(node_type), index, key))
        return Origin(document.path, position.line if position else None, position.col if position else None)

    for old, new in DEPRECATED_KEYS.get(node_type, {}).items():
        if old not in node:
            continue
        if new in node:
            yield D("SST-PRS020", origin=origin(old), subject=subject, artifact=subject, field=old, expected=new)
        elif node_type == "metric":
            yield D("SST-VAL122", origin=origin(old), subject=subject, metric=name)
        else:
            yield D(
                "SST-VAL012", origin=origin(old), subject=subject, type=node_type, name=name, field=old, expected=new
            )
    for key in INERT_KEYS.get(node_type, ()):
        if key not in node:
            continue
        yield D("SST-VAL211", origin=origin(key), subject=subject, relationship=name, field=key)
