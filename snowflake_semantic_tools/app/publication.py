"""Say why an artifact `sst test` or `sst eval` would check is not published as it compiles.

Each case has its own code and next step, so a target nothing was applied to reads as such
rather than as a plan gone stale: never published here, published from an older compile, or
published as compiled but changed outside SST since.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic


class Publication(Enum):
    """Whether one artifact is published on the target as the project compiles it."""

    PUBLISHED = "published"
    ABSENT = "absent"
    OLDER = "older"
    DRIFTED = "drifted"


@dataclass(frozen=True, slots=True)
class Checked:
    """One artifact `sst test` would probe or `sst eval` run, with how it is published.

    Attributes:
        key: The artifact's key, such as `semantic_view:sales`.
        target: The object's qualified name, as SQL.
    """

    key: str
    target: str
    publication: Publication


_CODES = {
    Publication.ABSENT: "SST-APL036",
    Publication.OLDER: "SST-APL038",
    Publication.DRIFTED: "SST-APL012",
}


def unpublished_findings(checked: Sequence[Checked], *, command: str) -> tuple[Diagnostic, ...]:
    """Report each checked artifact that is not published as compiled, in the order given.

    When none of them is published at all, one finding says so instead of one per artifact.

    Diagnostics:
        SST-APL037: no artifact `sst <command>` would check is published on the target; once.
        SST-APL036: an artifact is not published on the target.
        SST-APL038: an artifact is published from an older compile.
        SST-APL012: an artifact is published as compiled but its live object changed since.
    """
    if checked and all(item.publication is Publication.ABSENT for item in checked):
        return (D("SST-APL037", command=command, count=len(checked)),)
    return tuple(
        D(_CODES[item.publication], artifact=item.key, value=item.target)
        for item in checked
        if item.publication is not Publication.PUBLISHED
    )


def unpublished_count(checked: Sequence[Checked]) -> int:
    """Return how many checked artifacts are not published as compiled."""
    return sum(item.publication is not Publication.PUBLISHED for item in checked)
