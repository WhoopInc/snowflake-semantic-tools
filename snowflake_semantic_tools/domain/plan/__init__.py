"""Plan: decide what apply must do so Snowflake matches what compile rendered, without writing.

`changeset` builds a plan from its phases: `classify` decides each rendered artifact,
`prune` adds the orphans SST may drop and blocks the unsafe changes, and `order` puts the
changes in dependency order. This module re-exports what callers use, so importers never
reach into the submodules.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.plan.changeset import build_changeset
from snowflake_semantic_tools.domain.plan.order import dependency_waves, topological_order

__all__ = ["build_changeset", "dependency_waves", "topological_order"]
