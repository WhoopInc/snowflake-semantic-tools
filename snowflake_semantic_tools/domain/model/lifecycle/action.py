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

    STATE_MANIFEST_MISMATCH updates a composite artifact whose live resources already match
    the project while its state entry records another fingerprint or a failed write, so apply
    re-verifies it and records it without publishing. Another manifest having recorded an
    entry is no reason by itself: an artifact whose fingerprint matches is unchanged.
    UNMANAGED_OBJECT blocks an object that exists without a state entry, and TARGET_MOVED one
    whose entry records a different target.
    UNRECORDED_PUBLISH updates an object whose ownership marker names the very definition being
    planned, though state records an earlier one: an apply published it and was stopped before
    it recorded it, so publishing it again records it. An agent whose newest committed version
    already holds the specification is not given another version; only its properties are set.
    VERSION_DRIFT updates an agent that is otherwise unchanged whose alias or default version
    no longer points at the version holding its specification, or that has no live version,
    so apply points them back.
    OVERWRITE updates, under `--overwrite`, an object changed out of band that would otherwise
    be blocked: apply publishes the planned definition over the edit, and an agent keeps its
    versions and gains one, or only gets its marker back when its newest already holds it.
    DEFAULT_VERSION_DIFFERS updates a skill or plugin whose version is published but is not the
    extension's default, while no certified version outranks it, so apply makes it the default.
    DETACHED updates a CoWork profile that is otherwise unchanged but no longer attached to the
    account's CoWork object, so apply only attaches it again (Snowflake private preview).
    """

    NOT_PRESENT = "not_present"
    FINGERPRINT_DIFFERS = "fingerprint_differs"
    NO_PRIOR_STATE = "no_prior_state"
    STATE_MANIFEST_MISMATCH = "state_manifest_mismatch"
    UNCHANGED = "unchanged"
    ORPHANED = "orphaned"
    VALIDATION_ERRORS = "validation_errors"
    DEPENDENCY_BLOCKED = "dependency_blocked"
    UNMANAGED_OBJECT = "unmanaged_object"
    TARGET_MOVED = "target_moved"
    UNRECORDED_PUBLISH = "unrecorded_publish"
    VERSION_DRIFT = "version_drift"
    OVERWRITE = "overwrite"
    DEFAULT_VERSION_DIFFERS = "default_version_differs"
    DETACHED = "detached"
