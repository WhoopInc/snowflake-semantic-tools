"""Scope a plan by impact: the artifacts whose compiled form changed since a previous manifest.

`state:modified` selects what `modified_keys` returns. Without a previous manifest there is
nothing to compare with, so `impact_scope` falls back to the full plan and says so.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.state import Manifest

# The selector that asks for an impact-scoped plan.
STATE_MODIFIED = "state:modified"


def modified_keys(previous: Manifest, current: Manifest) -> frozenset[str]:
    """Return the keys of the artifacts `current` adds, or renders differently from `previous`."""
    return frozenset(
        key
        for key, entry in current.artifacts.items()
        if key not in previous.artifacts or previous.artifacts[key].fingerprint != entry.fingerprint
    )


def impact_scope(previous: Manifest | None, current: Manifest) -> tuple[frozenset[str] | None, Diagnostic | None]:
    """Return the keys an impact-scoped plan covers, or None for a full plan and why.

    Diagnostics:
        SST-PLN006: there is no previous manifest, so a full plan is computed.
    """
    if previous is None:
        return None, D("SST-PLN006")
    return modified_keys(previous, current), None
