"""What a plan decides for one artifact, and why.

Both enums' values are persisted: a saved plan records each change's action and reason
by value, and the plan JSON reports them, so a value never changes once released. A
rendered artifact picks its statements by action, which is why these live apart from
`change`: `rendered` and `change` both import them.
"""

from __future__ import annotations

from enum import Enum


class Action(Enum):
    """What apply does with one artifact.

    CREATE and UPDATE write the object, PRUNE removes an orphan SST owns, NOOP leaves an
    unchanged object alone, and BLOCKED writes nothing because of an error or a blocked
    dependency.
    """

    CREATE = "create"
    UPDATE = "update"
    NOOP = "noop"
    PRUNE = "prune"
    BLOCKED = "blocked"


class ChangeReason(Enum):
    """Why plan chose a change's action.

    STATE_MANIFEST_MISMATCH updates an object state recorded under another manifest, since
    its fingerprint may be stale. UNMANAGED_OBJECT blocks an object that exists without a
    state entry, and TARGET_MOVED one whose entry records a different target.
    """

    NOT_PRESENT = "not_present"
    FINGERPRINT_DIFFERS = "fingerprint_differs"
    NO_PRIOR_STATE = "no_prior_state"
    STATE_MANIFEST_MISMATCH = "state_manifest_mismatch"
    UNCHANGED = "unchanged"
    ORPHANED = "orphaned"
    POISONED_REFS = "poisoned_refs"
    VALIDATION_ERRORS = "validation_errors"
    DEPENDENCY_BLOCKED = "dependency_blocked"
    UNMANAGED_OBJECT = "unmanaged_object"
    TARGET_MOVED = "target_moved"
