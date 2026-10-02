"""Shape checks on authored nodes, so a key of the wrong type is reported rather than read silently."""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.defs import NULL_ORDERS, SORT_DIRECTIONS, _frame
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _load_nodes, _member_root, _node_origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.column_metadata import printable, synonym_problem
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, Origin


def _synonyms_diagnostics(
    value: object,
    *,
    artifact: str,
    subject: str,
) -> tuple[Diagnostic, ...]:
    """Check a `synonyms:` value: a list of strings, none holding a quote or a control character.

    An absent value is fine.

    Diagnostics:
        SST-PRS029: when the value is not a list of strings.
        SST-PRS030: when a synonym holds a quote or a control character, once per synonym.
    """
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        return (
            D(
                "SST-PRS029",
                artifact=artifact,
                found=type(value).__name__,
                subject=subject,
            ),
        )
    diagnostics = []
    for synonym in value:
        problem = synonym_problem(synonym)
        if problem is not None:
            diagnostics.append(
                D(
                    "SST-PRS030",
                    artifact=artifact,
                    value=printable(synonym),
                    detail=problem,
                    subject=subject,
                )
            )
    return tuple(diagnostics)


def _metric_parse_diagnostics(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
) -> tuple[Diagnostic, ...]:
    """Check the shape of every `snowflake_metrics:` entry, so a key of the wrong type is reported.

    Entries come from files in any folder, in document order; one with no name is called
    `<unnamed>`. Each runs its checks in order: `expr`, `tables`, `using_relationships`,
    `access_modifier`, `non_additive_dimensions`, `window`, then `synonyms`.

    Diagnostics:
        SST-PRS002: when there is no `expr`, a non-additive entry names no dimension, or a
            window `order_by` mapping has no string `ref`.
        SST-PRS113: when `expr` is not a string.
        SST-PRS003: when `tables`, `using_relationships`, `non_additive_dimensions`, `window`
            or a part of either block has the wrong type.
        SST-PRS013: when `access_modifier`, a `sort_direction` or a `null_order` is not an
            allowed value.
        SST-PRS014: when a window comes with `using_relationships` or `non_additive_dimensions`,
            or declares both `partition_by` and `partition_by_excluding`.
        SST-PRS124: when the window's `frame` is not a frame clause.
        SST-PRS029: when `synonyms` is not a list of strings.
        SST-PRS030: when a synonym holds a quote or a control character.
    """
    metrics_dir = project_dir / semantic_models_dir / "metrics"
    diagnostics: list[Diagnostic] = []
    allowed_access = ("private_access", "public_access")
    for document, index, node in _load_nodes(documents, metrics_dir, _member_root("metric")):
        name = str(node.get("name") or "<unnamed>")
        subject = artifact_key("metric", name)
        origin = _node_origin(document, _member_root("metric"), index)
        if "expr" not in node:
            diagnostics.append(
                D(
                    "SST-PRS002",
                    artifact=subject,
                    field="expr",
                    subject=subject,
                    origin=origin,
                )
            )
        elif not isinstance(node.get("expr"), str):
            diagnostics.append(
                D(
                    "SST-PRS113",
                    artifact=subject,
                    field="expr",
                    found=type(node.get("expr")).__name__,
                    subject=subject,
                    origin=origin,
                )
            )
        if "tables" in node and not isinstance(node.get("tables"), list):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field="tables",
                    expected="a list",
                    found=type(node.get("tables")).__name__,
                    subject=subject,
                    origin=origin,
                )
            )
        using = node.get("using_relationships")
        if using is not None and (not isinstance(using, list) or any(not isinstance(item, str) for item in using)):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field="using_relationships",
                    expected="a list of strings",
                    found=type(using).__name__,
                    subject=subject,
                    origin=origin,
                )
            )
        access = node.get("access_modifier")
        if access is not None and str(access) not in allowed_access:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=subject,
                    field="access_modifier",
                    found=access,
                    expected=", ".join(allowed_access),
                    subject=subject,
                    origin=origin,
                )
            )
        diagnostics.extend(_non_additive_parse_diagnostics(node.get("non_additive_dimensions"), subject, origin))
        diagnostics.extend(_window_parse_diagnostics(node, subject, origin))
        diagnostics.extend(_synonyms_diagnostics(node.get("synonyms"), artifact=subject, subject=subject))
    return tuple(diagnostics)


def _filter_parse_diagnostics(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
) -> tuple[Diagnostic, ...]:
    """Shape checks on filter keys whose wrong type would otherwise be read silently.

    Diagnostics:
        SST-PRS003: `labels` is not a list of strings.
    """
    filters_dir = project_dir / semantic_models_dir / "filters"
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, filters_dir, _member_root("filter")):
        labels = node.get("labels")
        if labels is None or (isinstance(labels, list) and all(isinstance(label, str) for label in labels)):
            continue
        subject = artifact_key("filter", node.get("name") or "<unnamed>")
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=subject,
                field="labels",
                expected="a list of strings",
                found=type(labels).__name__,
                subject=subject,
                origin=_node_origin(document, _member_root("filter"), index),
            )
        )
    return tuple(diagnostics)


def _window_parse_diagnostics(node: Mapping[str, Any], subject: str, origin: Origin) -> tuple[Diagnostic, ...]:
    """The shape of a metric's `window:` block; whether each entry resolves is checked with the models."""
    value = node.get("window")
    if value is None:
        return ()

    def wrong_type(field: str, expected: str, found: object) -> Diagnostic:
        return D(
            "SST-PRS003",
            artifact=subject,
            field=field,
            expected=expected,
            found=type(found).__name__,
            subject=subject,
            origin=origin,
        )

    if not isinstance(value, dict):
        return (wrong_type("window", "a mapping", value),)
    diagnostics: list[Diagnostic] = [
        # Snowflake's window metric grammar has neither clause.
        D("SST-PRS014", artifact=subject, field="window", other=other, subject=subject, origin=origin)
        for other in ("using_relationships", "non_additive_dimensions")
        if node.get(other)
    ]
    for field in ("partition_by", "partition_by_excluding"):
        entries = value.get(field)
        if entries is not None and (not isinstance(entries, list) or not all(isinstance(e, str) for e in entries)):
            diagnostics.append(wrong_type(f"window.{field}", "a list of references", entries))
    if value.get("partition_by") and value.get("partition_by_excluding"):
        diagnostics.append(
            D(
                "SST-PRS014",
                artifact=subject,
                field="window.partition_by",
                other="window.partition_by_excluding",
                subject=subject,
                origin=origin,
            )
        )
    order_by = value.get("order_by")
    if order_by is not None and not isinstance(order_by, list):
        diagnostics.append(wrong_type("window.order_by", "a list", order_by))
    for position, entry in enumerate(order_by if isinstance(order_by, list) else ()):
        diagnostics.extend(_order_by_entry(entry, f"window.order_by[{position}]", subject, origin, wrong_type))
    frame = value.get("frame")
    if frame is not None and _frame(frame) is None:
        diagnostics.append(D("SST-PRS124", artifact=subject, value=frame, subject=subject, origin=origin))
    return tuple(diagnostics)


def _order_by_entry(
    entry: object,
    field: str,
    subject: str,
    origin: Origin,
    wrong_type: Callable[[str, str, object], Diagnostic],
) -> list[Diagnostic]:
    """The shape of one `window.order_by` entry: a reference, or a mapping with a `ref` and sort keys."""
    if isinstance(entry, str):
        return []
    if not isinstance(entry, dict):
        return [wrong_type(field, "a reference or a mapping", entry)]
    diagnostics: list[Diagnostic] = []
    if not isinstance(entry.get("ref"), str):
        diagnostics.append(D("SST-PRS002", artifact=subject, field=f"{field}.ref", subject=subject, origin=origin))
    for key, allowed in (("sort_direction", SORT_DIRECTIONS), ("null_order", NULL_ORDERS)):
        if key in entry and (not isinstance(entry[key], str) or entry[key] not in allowed):
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=subject,
                    field=f"{field}.{key}",
                    found=entry[key],
                    expected=", ".join(allowed),
                    subject=subject,
                    origin=origin,
                )
            )
    return diagnostics


def _non_additive_parse_diagnostics(value: object, subject: str, origin: Origin) -> tuple[Diagnostic, ...]:
    """The shape of `non_additive_dimensions`; whether each entry resolves is checked with the models."""
    if value is None:
        return ()
    if not isinstance(value, list):
        return (
            D(
                "SST-PRS003",
                artifact=subject,
                field="non_additive_dimensions",
                expected="a list",
                found=type(value).__name__,
                subject=subject,
                origin=origin,
            ),
        )
    diagnostics: list[Diagnostic] = []
    for position, entry in enumerate(value):
        field = f"non_additive_dimensions[{position}]"
        if not isinstance(entry, dict):
            found = type(entry).__name__
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field=field,
                    expected="a mapping",
                    found=found,
                    subject=subject,
                    origin=origin,
                )
            )
            continue
        dimension = entry.get("dimension")
        if not isinstance(dimension, str) or not dimension.strip():
            diagnostics.append(
                D("SST-PRS002", artifact=subject, field=f"{field}.dimension", subject=subject, origin=origin)
            )
        table = entry.get("table")
        if "table" in entry and (not isinstance(table, str) or not table.strip() or "{{" in table):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field=f"{field}.table",
                    expected="a bare model name",
                    found=repr(table),
                    subject=subject,
                    origin=origin,
                )
            )
        for key, allowed in (("sort_direction", SORT_DIRECTIONS), ("null_order", NULL_ORDERS)):
            if key in entry and (not isinstance(entry[key], str) or entry[key] not in allowed):
                diagnostics.append(
                    D(
                        "SST-PRS013",
                        artifact=subject,
                        field=f"{field}.{key}",
                        found=entry[key],
                        expected=", ".join(allowed),
                        subject=subject,
                        origin=origin,
                    )
                )
    return tuple(diagnostics)


def _verified_query_diagnostics(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> tuple[Diagnostic, ...]:
    """Check that each `snowflake_verified_queries:` entry has exactly one SQL source, and a usable one.

    Entries come from files in any folder; one with no name is called `<unnamed>`. A
    `sql_file:` is resolved against the entry's own file.

    Raises:
        OSError: A `sql_file:` exists and cannot be read.

    Diagnostics:
        SST-VAL412: when an entry declares both `sql` and `sql_file`, or neither.
        SST-LOD018: when the `sql_file:` is not a file.
        SST-LOD019: when the `sql_file:` holds no bytes.
        SST-PRS122: when the `sql_file:` is not UTF-8.
    """
    root = project_dir / semantic_models_dir / "verified_queries"
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, root, _member_root("verified_query")):
        name = str(node.get("name") or "<unnamed>")
        subject = artifact_key("verified_query", name)
        origin = _node_origin(document, _member_root("verified_query"), index)
        has_sql = node.get("sql") is not None
        has_sql_file = node.get("sql_file") is not None
        if has_sql == has_sql_file:
            diagnostics.append(
                D(
                    "SST-VAL412",
                    member=name,
                    detail=("sql and sql_file are both present" if has_sql else "neither sql nor sql_file is present"),
                    subject=subject,
                    origin=origin,
                )
            )
            continue
        if has_sql_file:
            sql_path = document.abs_path.parent / str(node["sql_file"])
            if not sql_path.is_file():
                diagnostics.append(
                    D(
                        "SST-LOD018",
                        file=document.path,
                        path=str(node["sql_file"]),
                        subject=subject,
                        origin=origin,
                    )
                )
            elif not (content := sql_path.read_bytes()):
                diagnostics.append(
                    D(
                        "SST-LOD019",
                        path=str(node["sql_file"]),
                        file=document.path,
                        subject=subject,
                        origin=origin,
                    )
                )
            else:
                diagnostics.extend(_utf8_diagnostics(content, document.path, str(node["sql_file"]), subject, origin))
    return tuple(diagnostics)


def _utf8_diagnostics(
    content: bytes, document_path: str, sql_file: str, subject: str, origin: Origin
) -> tuple[Diagnostic, ...]:
    """Report SST-PRS122 when `content` is not UTF-8, naming the file as its document's sibling path."""
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as exc:
        file = posixpath.normpath(posixpath.join(posixpath.dirname(document_path), sql_file))
        return (D("SST-PRS122", file=file, offset=exc.start, subject=subject, origin=origin),)
    return ()
