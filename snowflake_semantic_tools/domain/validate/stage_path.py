"""The file-name rule every staged file must satisfy.

Upload refuses a path with any other character, so validation applies the same
rule offline: a name that would fail at upload fails before anything is written.
"""

from __future__ import annotations

SAFE_SEGMENT_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-$")
ALLOWED_DESCRIPTION = "letters, digits, '.', '_', '-', or '$'"


def unsafe_segment(path: str) -> str | None:
    """The first segment of a relative path that cannot be staged, or None."""
    for part in path.split("/"):
        if not part or part in (".", "..") or any(character not in SAFE_SEGMENT_CHARACTERS for character in part):
            return part
    return None
