"""Immutable lifecycle values shared by plan, apply, and the adapters.

Compile renders each artifact as a `RenderedArtifact`; plan compares it with what Snowflake
shows, a `SnowflakeObservation`, and decides one `Change` per artifact, collected in a
`ChangeSet`; apply runs the writes and reports an `ApplyOutcome` for each. One submodule
holds each stage's values -- `marker`, `observation`, `action`, `rendered`, `change`,
`apply`, `version`, and `drift` -- and this module re-exports them, so importers never reach into the submodules.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.lifecycle.action import Action, ChangeReason
from snowflake_semantic_tools.domain.model.lifecycle.apply import (
    ApplyOptions,
    ApplyOutcome,
    ApplyResult,
    ClassifiedError,
    ErrorKind,
    FailurePolicy,
    GrantCheck,
    OutcomeStatus,
    RetryPolicy,
)
from snowflake_semantic_tools.domain.model.lifecycle.change import Change, ChangeSet, CompositePlan
from snowflake_semantic_tools.domain.model.lifecycle.drift import (
    DEFINED_TYPES,
    DESCRIBED_TYPES,
    DIGESTED_TYPES,
    DRIFT_CHECK_BASIC,
    DRIFT_CHECK_FULL,
    DRIFT_CHECKS,
    SHOWN_TYPES,
    drift_properties,
    drifted_properties,
    live_digest,
)
from snowflake_semantic_tools.domain.model.lifecycle.marker import OwnershipMarker, extract_marker, marked_comment
from snowflake_semantic_tools.domain.model.lifecycle.observation import (
    ArtifactKey,
    CompositeObservation,
    ExecResult,
    ExecutionError,
    GrantRow,
    ObservedArtifact,
    PhysicalResource,
    QueryResult,
    ShowRow,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.lifecycle.rendered import (
    SSE_STAGE_TYPE,
    CompositeFacts,
    DesiredMetadata,
    ProbeKind,
    PublishShape,
    RenderedArtifact,
    SmokeProbe,
    StatementPlan,
    Upload,
    version_drift,
)
from snowflake_semantic_tools.domain.model.lifecycle.version import (
    commented_spec_fingerprint,
    held_spec_fingerprint,
    spec_fingerprint,
    version_comment,
)

__all__ = [
    "DEFINED_TYPES",
    "DESCRIBED_TYPES",
    "DIGESTED_TYPES",
    "DRIFT_CHECKS",
    "DRIFT_CHECK_BASIC",
    "DRIFT_CHECK_FULL",
    "SHOWN_TYPES",
    "SSE_STAGE_TYPE",
    "Action",
    "ApplyOptions",
    "ApplyOutcome",
    "ApplyResult",
    "ArtifactKey",
    "Change",
    "ChangeReason",
    "ChangeSet",
    "ClassifiedError",
    "CompositeFacts",
    "CompositeObservation",
    "CompositePlan",
    "DesiredMetadata",
    "ErrorKind",
    "ExecResult",
    "ExecutionError",
    "FailurePolicy",
    "GrantCheck",
    "GrantRow",
    "ObservedArtifact",
    "OutcomeStatus",
    "OwnershipMarker",
    "PhysicalResource",
    "ProbeKind",
    "PublishShape",
    "QueryResult",
    "RenderedArtifact",
    "RetryPolicy",
    "ShowRow",
    "SmokeProbe",
    "SnowflakeObservation",
    "StatementPlan",
    "Upload",
    "commented_spec_fingerprint",
    "drift_properties",
    "drifted_properties",
    "extract_marker",
    "held_spec_fingerprint",
    "live_digest",
    "marked_comment",
    "spec_fingerprint",
    "version_comment",
    "version_drift",
]
