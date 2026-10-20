"""The comment SST gives each agent version it commits, and the specification fingerprint in it.

Every version SST commits carries `git:<sha> sst-spec:<digest>`: the commit it was published
from, `<sha>-dirty` when the project had uncommitted changes, and the fingerprint of its
specification alone. Plan reads the fingerprint back from
SHOW VERSIONS IN AGENT to tell whether the newest committed version already holds the
specification being planned, so a change to only the agent's comment, profile, tags, alias or
`secure` moves no version. A version committed before SST wrote the fingerprint is recognised
by its specification instead, when the role may read it.
"""

from __future__ import annotations

import json
import re
from hashlib import sha256

# What precedes the specification fingerprint in a version's comment.
SPEC_TOKEN = "sst-spec:"
# What follows the sha of a version published while the project had uncommitted changes.
DIRTY_SUFFIX = "-dirty"
_SPEC = re.compile(r"(?:^|\s)sst-spec:([0-9a-f]{64})(?=\s|$)", re.IGNORECASE)


def spec_fingerprint(specification: str) -> str:
    """Fingerprint an agent specification: the SHA-256 of its canonical JSON, keys sorted.

    Text that is not JSON is fingerprinted as written, its lines right-stripped, so it matches
    no rendered specification unless it is the same text.
    """
    try:
        document = json.loads(specification)
        text = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except ValueError:
        text = "\n".join(line.rstrip() for line in specification.splitlines()).strip()
    return sha256(text.encode("utf-8")).hexdigest()


def version_comment(git_sha: str, specification: str, *, dirty: bool = False) -> str:
    """Return the comment a committed version carries: `git:<sha> sst-spec:<fingerprint>`.

    A `dirty` version, published while the project had uncommitted changes, reads
    `git:<sha>-dirty`, since the commit does not hold its specification.
    """
    label = f"{git_sha}{DIRTY_SUFFIX}" if dirty else git_sha
    return f"git:{label} {SPEC_TOKEN}{spec_fingerprint(specification)}"


def commented_spec_fingerprint(comment: str | None) -> str | None:
    """Read the specification fingerprint from a version's comment; None when it carries none."""
    match = _SPEC.search(comment or "")
    return match.group(1).lower() if match else None


def held_spec_fingerprint(comment: str | None, specification: str | None) -> str | None:
    """Return the fingerprint of the specification a committed version holds.

    The comment's fingerprint wins, since SST wrote it with the version; a version without one
    is fingerprinted from its specification. None when neither is there, as for a secure
    agent's version the role may not read.
    """
    commented = commented_spec_fingerprint(comment)
    if commented is not None:
        return commented
    return spec_fingerprint(specification) if specification is not None else None
