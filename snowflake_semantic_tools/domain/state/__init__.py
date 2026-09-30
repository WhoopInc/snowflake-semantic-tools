"""Pure manifest, state, and saved-plan documents, and what they share.

`manifest` holds the compile manifest, `applied` the state apply records per target, and
`saved_plan` a plan saved for a later apply; `documents` and `codec` hold what they all
share: the canonical encoding, the content hash, the error a stored document raises, and the
field shapes. This module re-exports their public names, so importers never reach into the
submodules.
"""

from __future__ import annotations

from .applied import (
    APPLIED,
    DEACTIVATED,
    FAILED_AFTER_WRITE,
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    AppliedResource,
    AppliedResourceInput,
    LastRun,
    ResourceStatus,
    State,
    migrate_state,
)
from .codec import optional_object, pairs_from_json, pairs_to_json, resources_from_json, resources_to_json
from .documents import SST_VERSION, StoredDocumentError, canonical_json, content_hash
from .manifest import MANIFEST_SCHEMA_VERSION, ArtifactEntry, ImpactIndex, Manifest, migrate_manifest
from .saved_plan import PLAN_SCHEMA_VERSION, SavedChange, SavedPlan

__all__ = [
    "APPLIED",
    "AppliedEntry",
    "AppliedResource",
    "AppliedResourceInput",
    "ArtifactEntry",
    "DEACTIVATED",
    "FAILED_AFTER_WRITE",
    "ImpactIndex",
    "LastRun",
    "MANIFEST_SCHEMA_VERSION",
    "Manifest",
    "PLAN_SCHEMA_VERSION",
    "ResourceStatus",
    "SST_VERSION",
    "STATE_SCHEMA_VERSION",
    "SavedChange",
    "SavedPlan",
    "State",
    "StoredDocumentError",
    "canonical_json",
    "content_hash",
    "migrate_manifest",
    "migrate_state",
    "optional_object",
    "pairs_from_json",
    "pairs_to_json",
    "resources_from_json",
    "resources_to_json",
]
