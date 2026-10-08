"""Report what a plan decided: one notice per unchanged artifact, and a summary of the counts.

`plan_notices` reads a finished plan and never changes it. The summary counts each artifact
type's creates, updates, unchanged artifacts, and prunes, types in name order. An update is
counted as a `replace` for a type an update replaces, and as an `update` for one an update
alters or re-versions in place, such as an agent, which gains a new version.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeSet
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

_COUNTED = ((Action.CREATE, "create"), (Action.UPDATE, None), (Action.NOOP, "noop"), (Action.PRUNE, "prune"))


def plan_notices(changeset: ChangeSet) -> tuple[Diagnostic, ...]:
    """Report each unchanged artifact, in plan order, then the plan's per-type counts.

    Returns:
        Nothing for a plan with no changes, such as one a dependency cycle stopped.

    Diagnostics:
        SST-PLN015: an artifact's proposed definition matches what Snowflake holds.
        SST-PLN016: how many creates, replaces or updates, unchanged artifacts, and prunes each
            type has.
    """
    if not changeset.changes:
        return ()
    unchanged = tuple(
        D("SST-PLN015", subject=change.key, artifact=change.key)
        for change in changeset.changes
        if change.action is Action.NOOP
    )
    return (*unchanged, D("SST-PLN016", value=plan_summary(changeset)))


def plan_summary(changeset: ChangeSet) -> str:
    """Return each type's counts, as `view: 1 create, 0 replace, 2 noop, 0 prune`, joined by `; `.

    A type an update does not replace, such as `agent`, counts its updates as `update`.
    """
    types = sorted({change.artifact_type for change in changeset.changes})
    return "; ".join(
        f"{artifact_type}: "
        + ", ".join(
            f"{sum(change.artifact_type == artifact_type and change.action is action for change in changeset.changes)} "
            f"{label or _update_label(artifact_type)}"
            for action, label in _COUNTED
        )
        for artifact_type in types
    )


def _update_label(artifact_type: str) -> str:
    """Name an update of `artifact_type`: `replace` unless the registry says it updates in place."""
    registered = SEMANTIC_REGISTRY.artifacts.get(artifact_type)
    return "update" if registered is not None and not registered.replaces_on_update else "replace"
