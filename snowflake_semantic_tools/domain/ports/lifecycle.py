"""Injected lifecycle seam for logical artifacts backed by multiple resources."""

from __future__ import annotations

from typing import Protocol

from ..model.lifecycle import ApplyOptions, ApplyOutcome, Change, CompositePlan, RenderedArtifact
from ..state import AppliedEntry, AppliedResourceInput, Manifest


class CompositeLifecycleHandler(Protocol):
    """Plan and apply one composite artifact type, whose objects no single statement publishes.

    Plan and apply hand every artifact of the type to its handler instead of the generic
    path: the handler observes the objects itself, decides the change, publishes, and says
    which resources state records. Observation never sees a composite artifact, so its
    prune comes from its state entry, through `report_prune`.

    Attributes:
        artifact_type: The artifact type the handler owns; plan and apply route each artifact
            of that type to it.
    """

    artifact_type: str

    def plan(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        manifest: Manifest,
    ) -> CompositePlan:
        """Decide one artifact's change from what Snowflake holds and what state recorded for it.

        Reads Snowflake and never writes it. Snowflake refusing an observation does not raise:
        the plan blocks the artifact instead.

        Args:
            state_entry: What state recorded for the artifact; None when it records nothing.
            manifest: The manifest being planned.

        Returns:
            The action and its reason, with what was observed, which apply checks again before
            it publishes, and the diagnostics the decision reported.

        Diagnostics:
            SST-PLN001: Snowflake refused an observation, which blocks the artifact.
            Each handler's decision reports its own diagnostics besides.
        """
        ...

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Carry out one planned change of the handler's type, and report how it ended.

        Every change of the type comes here, a NOOP, a BLOCKED change, and a prune included;
        one with nothing to run is skipped, and an executable prune runs only when `options`
        allow pruning. A failure is reported in the outcome, with whether anything was written
        and which resources were verified, since state records exactly those: an exception
        that escapes fails the change as though nothing was written.
        """
        ...

    def merge_physical_resources(
        self,
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[AppliedResourceInput, ...]:
        """Return the physical resources state records for an artifact the run wrote.

        Called for a change that applied, or that failed after writing. Pure: it reads nothing
        from Snowflake.

        Args:
            current: The `(object type, qualified name)` pairs the outcome verified. Empty is
                authoritative: the run verified none, and nothing rendered is assumed instead.
            previous: The entry state held for the artifact before the run; None when none.

        Returns:
            What the new entry records: `current`, and any earlier resource the handler keeps
            as SST's although the run did not verify it.
        """
        ...

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        """Return the prune of an artifact state records but the plan no longer renders.

        Built from the state entry alone, since observation never sees a composite artifact:
        Snowflake is neither read nor written. Plan asks only about an entry that is not
        already deactivated. The handler decides whether apply may execute the prune or only
        reports it, as the change's `prune_executable` says.
        """
        ...
