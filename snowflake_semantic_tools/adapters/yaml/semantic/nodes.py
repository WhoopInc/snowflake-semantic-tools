"""Walk semantic-model nodes: where each one sits, its strings, and the tables it attaches to."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ....domain.model.diagnostic import D, Origin
from ....domain.model.reference import TemplateSyntaxError, single_template_call
from ....domain.model.registry import SEMANTIC_REGISTRY
from ...errors import ProjectError
from ..documents import NodePath, RawDocument, RawDocuments


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    """Normalise a YAML scalar-or-list into a tuple of strings.

    Numbers are stringified because `sample_values: [1100, 700]` is authored as
    integers but renders as `'1100', '700'` -- YAML typing is not SQL typing.
    """
    if value is None:
        return ()
    if isinstance(value, bool):
        return ("true" if value else "false",)
    if isinstance(value, (str, int, float)):
        return (str(value),)
    if isinstance(value, list):
        return tuple("true" if v is True else "false" if v is False else str(v) for v in value)
    raise ProjectError(f"expected a scalar or list, found {type(value).__name__}")


def _list_of(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _table_refs(value: object) -> tuple[str, ...]:
    refs: list[str] = []
    for raw in value if isinstance(value, list) else []:
        try:
            call = single_template_call(str(raw), "ref")
        except TemplateSyntaxError as exc:
            diagnostic = D(
                "SST-LOD004",
                file="<tables>",
                line=exc.line,
                col=exc.col,
                reason=exc.reason,
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
        if call is not None:
            if len(call.args) != 1:
                raise ProjectError(f"table attachment ref() must have one argument, found {raw!r}")
            refs.append(call.args[0].casefold())
            continue
        legacy = single_template_call(str(raw), "table")
        if legacy is not None and len(legacy.args) == 1:
            refs.append(legacy.args[0].casefold())
            continue
        literal = str(raw).strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", literal):
            raise ProjectError(f"table attachment must be a model name, found {raw!r}")
        refs.append(literal.casefold())
    return tuple(refs)


def _safe_table_refs(value: object) -> tuple[str, ...]:
    try:
        return _table_refs(value)
    except ProjectError:
        return ()


def _table_refs_poisoned(value: object) -> bool:
    try:
        _table_refs(value)
    except ProjectError:
        return True
    return False


def _node_root(node_type: str) -> str:
    if node_type == "semantic_view":
        root_key = SEMANTIC_REGISTRY.artifacts[node_type].root_key
        assert root_key is not None
        return root_key
    return _member_root(node_type)


def _node_origin(document: RawDocument, root_key: str, index: int, field: str | None = None) -> Origin:
    path: NodePath = (root_key, index) if field is None else (root_key, index, field)
    position = document.position(path)
    return Origin(
        file=document.path,
        line=position.line if position is not None else None,
        col=position.col if position is not None else None,
    )


def _load_nodes(
    documents: RawDocuments, directory: Path, root_key: str
) -> tuple[tuple[RawDocument, int, dict[str, Any]], ...]:
    del directory
    loaded: list[tuple[RawDocument, int, dict[str, Any]]] = []
    for document in documents.documents:
        if root_key not in document.root_keys:
            continue
        for index, node in enumerate(document.tree.get(root_key) or []):
            if isinstance(node, dict):
                loaded.append((document, index, _normalise_node_strings(node)))
    return tuple(loaded)


def _member_root(name: str) -> str:
    return SEMANTIC_REGISTRY.members[name].root_key


def _normalise_node_strings(node: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): (
            " ".join(value.splitlines()).strip() if str(key) == "description" and isinstance(value, str) else value
        )
        for key, value in node.items()
    }
