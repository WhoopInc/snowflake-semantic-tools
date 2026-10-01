"""Guarded execution of a reviewed ChangeSet.

`ApplyArtifacts` runs a plan's writes under the apply lock and records them in state. Modules,
in the order a run reaches them: `run` (the run's phases and its dependency waves), `one` (one
change: re-checking the plan, executing with retries, verifying the write), `errors`
(classifying a failure and reporting it as a diagnostic), `state` (the entries a run leaves).
"""

from __future__ import annotations

from snowflake_semantic_tools.app.apply.errors import classify_error
from snowflake_semantic_tools.app.apply.one import preserves_grants
from snowflake_semantic_tools.app.apply.run import ApplyArtifacts

__all__ = ["ApplyArtifacts", "classify_error", "preserves_grants"]
