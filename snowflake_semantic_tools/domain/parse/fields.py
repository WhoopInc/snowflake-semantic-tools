"""Name an unknown field: as a likely typo of a field SST knows, or as simply unknown."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic


def edit_distance(a: str, b: str) -> int:
    """The optimal string alignment distance: insertions, deletions, substitutions, transpositions."""
    previous: list[int] | None = None
    current = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        before, previous, current = previous, current, [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            current[j] = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
            if before is not None and i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                current[j] = min(current[j], before[j - 2] + 1)
    return current[len(b)]


def nearest_field(field: str, known: Iterable[str]) -> str | None:
    """The known field `field` is most likely a typo of, or None when none is close.

    Close is one edit for a field of up to four characters and two for a longer one, compared
    casefolded; the nearest wins, then the first in sorted order.
    """
    folded = field.casefold()
    limit = 1 if len(folded) <= 4 else 2
    scored = sorted((edit_distance(folded, name.casefold()), name) for name in set(known) if name.casefold() != folded)
    return next((name for distance, name in scored if distance <= limit), None)


def unknown_field(field: str, known: Iterable[str], *, label: str | None = None, **context: Any) -> Diagnostic:
    """Report a field its block does not model, as a typo when it is close to one it does.

    Args:
        field: The unknown key, as the block writes it.
        known: The keys the block models.
        label: How the message names the field, such as `window.order_by[0].colum`; `field` when None.
        context: The rest of the diagnostic: `artifact`, and optionally `origin` and `subject`.

    Diagnostics:
        SST-PRS022: the field is within edit distance of a known one, which it names.
        SST-PRS004: otherwise.
    """
    named = label if label is not None else field
    suggestion = nearest_field(field, known)
    if suggestion is None:
        return D("SST-PRS004", field=named, **context)
    prefix = named[: len(named) - len(field)] if named.endswith(field) else ""
    return D("SST-PRS022", field=named, expected=f"{prefix}{suggestion}", **context)
