"""What `sst state recover` finds for an artifact state does not record, and a handler's proof.

An object SST published proves it is SST's by the ownership marker in its comment. A composite
artifact's objects carry none, so its lifecycle handler proves it from their content instead
and answers with a `RecoveryProof`. Each `found` value below says what recover found: that it
recorded the artifact, or why it could not.
"""

from __future__ import annotations

from dataclasses import dataclass

RECORDED = "recorded"
NO_MARKER = "no marker"
OTHER_DEFINITION = "marker for another definition"
ABSENT = "absent"
UNMARKED = "no marker on Snowflake"
BUNDLE_DIFFERS = "bundle differs"
BUNDLE_PREFIX_MISSING = "bundle prefix missing"
ROW_DIFFERS = "row differs"
PROVENANCE_DIFFERS = "provenance differs"
CONFIG_DIFFERS = "config differs"
UNOBSERVABLE = "unobservable"


@dataclass(frozen=True, slots=True)
class RecoveryProof:
    """What a composite handler found when asked to prove an unrecorded artifact is SST's.

    Attributes:
        found: `RECORDED` when the live objects are exactly the compiled definition; otherwise
            one of the other values above, saying why not.
        component_fingerprints: The parts state records, as apply records them for a publish
            of the same definition; empty unless recorded.
        physical_resources: The `(object type, qualified name)` pairs state records, as apply
            records them; empty unless recorded, and for an eval whose dataset SST did not mint.
        qualified_name: The object recover reports the artifact under; empty for the artifact's
            compiled target.
    """

    found: str
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, str], ...] = ()
    qualified_name: str = ""

    @property
    def recorded(self) -> bool:
        """Report whether the proof holds, so recover records the artifact."""
        return self.found == RECORDED
