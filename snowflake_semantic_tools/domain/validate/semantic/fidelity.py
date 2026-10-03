"""Report each authored value the loader reads but the renderer would drop or change.

The loader keeps only what Snowflake's grammar can carry: a filter's `labels:` reach the
DDL as `LABELS = (FILTER)` and nothing else, and a verified query's `verified_by` is read
as trimmed text. A value that does not survive that is reported, so a review does not
take it for published.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import AuthoredDocuments
from snowflake_semantic_tools.domain.validate.semantic.nodes import load_nodes, member_root, node_origin

# The one label `LABELS = (...)` renders; every other label is read and dropped.
RENDERED_LABEL = "filter"


def renderer_fidelity_diagnostics(documents: AuthoredDocuments) -> tuple[Diagnostic, ...]:
    """Report, filters then verified queries in document order, each value the renderer would not keep.

    Diagnostics:
        SST-VAL011: a filter label other than `filter`, which the renderer drops; or a
            `verified_by` that is not text, or carries surrounding whitespace, which the
            renderer converts, trims or drops.
    """
    return (*_filter_labels(documents), *_verifiers(documents))


def _filter_labels(documents: AuthoredDocuments) -> Iterator[Diagnostic]:
    root = member_root("filter")
    for document, index, node in load_nodes(documents, root):
        labels = node.get("labels")
        if not node.get("name") or not isinstance(labels, list):
            continue
        name = str(node["name"])
        origin = node_origin(document, root, index)
        for label in labels:
            # A label that is not text is SST-PRS003's to report.
            if isinstance(label, str) and label.casefold() != RENDERED_LABEL:
                yield _dropped("filter", name, f"labels: {label}", "dropped", origin)


def _verifiers(documents: AuthoredDocuments) -> Iterator[Diagnostic]:
    root = member_root("verified_query")
    for document, index, node in load_nodes(documents, root):
        detail = verifier_change(node)
        if node.get("name") and detail is not None:
            yield _dropped(
                "verified_query", str(node["name"]), "verified_by", detail, node_origin(document, root, index)
            )


def verifier_change(node: Mapping[str, Any]) -> str | None:
    """Say what the loader would do to `verified_by`; None when it reaches the DDL as written."""
    value = node.get("verified_by")
    if value is None:
        return None
    if not isinstance(value, str):
        # The loader reads `str(value or "")`, so a falsy value is dropped and any other is rewritten.
        return "converted to text" if value else "dropped"
    stripped = value.strip()
    if stripped == value:
        return None
    return "trimmed" if stripped else "dropped"


def _dropped(node_type: str, name: str, field: str, detail: str, origin: Origin) -> Diagnostic:
    return D(
        "SST-VAL011",
        origin=origin,
        subject=artifact_key(node_type, name),
        type=node_type,
        name=name,
        field=field,
        detail=detail,
    )
