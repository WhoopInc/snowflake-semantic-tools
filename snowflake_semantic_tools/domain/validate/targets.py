"""Check what the compiled artifacts publish to, across every artifact type."""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName


def shared_targets(published: Sequence[tuple[str, str, QualifiedName]]) -> tuple[Diagnostic, ...]:
    """Report two artifacts of different types that publish to one Snowflake name, once per name.

    Whether the object types share a namespace is not documented for every pair, so this
    warns rather than refuses; it always makes a report ambiguous.

    Args:
        published: Each artifact as its type, its artifact key, and its target, in stream order;
            the first two artifacts on a name are the ones reported.

    Diagnostics:
        SST-VAL843: two artifacts of different types publish to one Snowflake name.
    """
    by_target: dict[tuple[str, str, str], list[tuple[str, str, QualifiedName]]] = {}
    for item in published:
        by_target.setdefault(item[2].folded, []).append(item)
    # Profiles share the registry table by design, so only a clash across types counts.
    return tuple(
        D("SST-VAL843", subject=items[1][1], a=items[0][1], b=items[1][1], target=items[0][2].sql)
        for items in by_target.values()
        if len({item[0] for item in items}) > 1
    )
