"""Immutable lifecycle values shared by plan, apply, and the adapters.

Compile renders each artifact as a `RenderedArtifact`; plan compares it with what Snowflake
shows, a `SnowflakeObservation`, and decides one `Change` per artifact, collected in a
`ChangeSet`; apply runs the writes and reports an `ApplyOutcome` for each. One submodule
holds each stage's values -- `marker`, `observation`, `action`, `rendered`, `change`, and
`apply` -- and this module re-exports them, so importers never reach into the submodules.
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
from snowflake_semantic_tools.domain.model.lifecycle.marker import OwnershipMarker, extract_marker
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
    CompositeFacts,
    DesiredMetadata,
    ProbeKind,
    PublishShape,
    RenderedArtifact,
    SmokeProbe,
    StatementPlan,
    Upload,
)

__all__ = [
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
    "extract_marker",
]
