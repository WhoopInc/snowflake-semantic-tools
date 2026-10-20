"""Report what a plan decided: one notice per unchanged artifact, and a summary of the counts.

`plan_notices` reads a finished plan and never changes it. It first warns when the plan commits
agent versions while the project has uncommitted changes, which label those versions dirty.
The summary counts each artifact
type's creates, updates, unchanged artifacts, and prunes, types in name order. An update is
counted as a `replace` for a type an update replaces, and as an `update` for one an update
alters or re-versions in place, such as an agent, which gains a new version.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeSet
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

_COUNTED = ((Action.CREATE, "create"), (Action.UPDATE, None), (Action.NOOP, "noop"), (Action.PRUNE, "prune"))
# How many uncommitted paths SST-PLN042 names before it counts the rest.
_SHOWN_PATHS = 5


def plan_notices(changeset: ChangeSet, uncommitted: tuple[str, ...] = ()) -> tuple[Diagnostic, ...]:
    """Report a dirty work tree, then each unchanged artifact, in plan order, then the per-type counts.

    Args:
        uncommitted: The project's uncommitted paths, as `ProjectInputs.git_dirty` lists them.

    Returns:
        Nothing for a plan with no changes, such as one a dependency cycle stopped.

    Diagnostics:
        SST-PLN042: the plan commits an agent version while the project has uncommitted paths.
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
    return (*_dirty_tree(changeset, uncommitted), *unchanged, D("SST-PLN016", value=plan_summary(changeset)))


def _dirty_tree(changeset: ChangeSet, uncommitted: tuple[str, ...]) -> tuple[Diagnostic, ...]:
    """Warn once when the plan commits an agent version while the project has uncommitted paths."""
    versions = sum(_commits_version(change) for change in changeset.changes)
    if not uncommitted or not versions:
        return ()
    rest = len(uncommitted) - _SHOWN_PATHS
    shown = ", ".join(uncommitted[:_SHOWN_PATHS]) + (f", and {rest} more" if rest > 0 else "")
    return (D("SST-PLN042", count=versions, value=shown),)


def _commits_version(change: Change) -> bool:
    """Whether the change commits a version of a permanent agent: creates one, or adds a version."""
    rendered = change.rendered
    if rendered is None or rendered.object_type != "AGENT" or rendered.temporary:
        return False
    return change.action is Action.CREATE or rendered.adds_version is True


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
