"""The ownership marker SST writes into a published object's comment, and how to read it back.

A marker is `[sst:<manifest_id>:<fingerprint>]`: the manifest that published the object
and the fingerprint of what it published, each a 64-character hex digest. Plan treats an
object as SST's only when its comment carries a marker that matches state, so this text
form is a contract with every object SST has already published.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_MARKER = re.compile(r"\[sst:([0-9a-f]{64}):([0-9a-f]{64})\]", re.IGNORECASE)


@dataclass(frozen=True, slots=True, order=True)
class OwnershipMarker:
    """The manifest and fingerprint an SST-published object records in its comment.

    Both digests are stored lowercase, so markers compare equal whatever case a comment
    spelled them in.

    Raises:
        ValueError: either value is not a 64-character hexadecimal digest.
    """

    manifest_id: str
    fingerprint: str

    def __post_init__(self) -> None:
        for field_name, value in (("manifest_id", self.manifest_id), ("fingerprint", self.fingerprint)):
            if not re.fullmatch(r"[0-9a-f]{64}", value, re.IGNORECASE):
                raise ValueError(f"{field_name} must be a 64-character hexadecimal digest")
        object.__setattr__(self, "manifest_id", self.manifest_id.lower())
        object.__setattr__(self, "fingerprint", self.fingerprint.lower())

    @property
    def text(self) -> str:
        """Spell the marker as a comment carries it: `[sst:<manifest_id>:<fingerprint>]`."""
        return f"[sst:{self.manifest_id.lower()}:{self.fingerprint.lower()}]"


def extract_marker(comment: str | None) -> OwnershipMarker | None:
    """Read the ownership marker from an object's comment; the last one wins when there are several.

    Returns:
        The marker, or None when the comment is absent or carries no marker.
    """
    if comment is None:
        return None
    matches = tuple(_MARKER.finditer(comment))
    if not matches:
        return None
    match = matches[-1]
    return OwnershipMarker(match.group(1).lower(), match.group(2).lower())
