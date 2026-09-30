"""Injected lifecycle seam for logical artifacts backed by multiple resources."""

from __future__ import annotations

from typing import Protocol

from ..model.lifecycle import ApplyOptions, ApplyOutcome, Change, CompositePlan, RenderedArtifact
from ..state import AppliedEntry, AppliedResourceInput, Manifest


class CompositeLifecycleHandler(Protocol):
    artifact_type: str

    def plan(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        manifest: Manifest,
    ) -> CompositePlan: ...

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome: ...

    def merge_physical_resources(
        self,
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[AppliedResourceInput, ...]: ...

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change: ...
