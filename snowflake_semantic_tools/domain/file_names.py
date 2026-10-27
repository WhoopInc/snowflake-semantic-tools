"""File names derived from artifact names: one segment, inside the folder it is written to.

An artifact's name is authored, and a quoted one may hold any character, a `/` or a `..`
among them. Every file SST names after an artifact takes its name from `file_name`, so no name
can reach another folder, and two different names never share a file.
"""

from __future__ import annotations

import hashlib
import string

# Kept as they are; every other character is written as `%XX`, one per UTF-8 byte.
_KEPT = frozenset(string.ascii_letters + string.digits + "_-.")

# Below the 255 bytes most filesystems allow a name, leaving room for a suffix.
MAX_FILE_NAME = 200
_DIGEST = 16


def file_name(name: str) -> str:
    """Return `name` as one safe path segment, which the caller suffixes with `.sql` or `.json`.

    A character outside `[A-Za-z0-9_.-]` is percent-encoded (so `%` itself is), as is a
    leading `.`, so the result is never empty, hidden, `.` or `..`, and holds no separator.
    Distinct names give distinct segments, except that one longer than `MAX_FILE_NAME` is cut
    and ends in a hash of the whole name.
    """
    encoded = "".join(
        character
        if character in _KEPT
        else "".join(f"%{byte:02X}" for byte in character.encode("utf-8", "surrogatepass"))
        for character in name
    )
    if encoded.startswith("."):
        encoded = "%2E" + encoded[1:]
    if not encoded:
        return "%"
    if len(encoded) > MAX_FILE_NAME:
        digest = hashlib.sha256(name.encode("utf-8", "surrogatepass")).hexdigest()[:_DIGEST]
        encoded = f"{encoded[: MAX_FILE_NAME - _DIGEST - 1]}-{digest}"
    return encoded
