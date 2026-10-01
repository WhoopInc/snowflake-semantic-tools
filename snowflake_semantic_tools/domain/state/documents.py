"""What every document SST persists shares: its canonical encoding, content hash, and read error.

Manifests, local state, Snowflake state rows, and saved plans are written as
`canonical_json`, so equal values give equal bytes, and a document's id is the
`content_hash` of that encoding. Changing the encoding would make every stored manifest and
saved plan fail its id check on the next read.
"""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any

from snowflake_semantic_tools._version import __version__

# The version every document records as its writer: a manifest's generator, a run's state.
SST_VERSION = __version__


def canonical_json(value: object) -> bytes:
    """Encode a JSON value canonically: sorted keys, no whitespace, UTF-8, and no NaN or infinity.

    Raises:
        ValueError: the value holds NaN or an infinity, which JSON cannot carry.
        TypeError: the value holds something JSON cannot encode.
    """
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def content_hash(value: object) -> str:
    """Hash a value's canonical encoding with SHA-256, as SST computes every document id."""
    return sha256(canonical_json(value)).hexdigest()


class StoredDocumentError(ValueError):
    """A stored manifest or state document SST cannot use, and the code that names why.

    The store that read the file adds its path when it turns this into a diagnostic.

    Attributes:
        code: The diagnostic code to report.
        context: The code's template values, without the path.
    """

    def __init__(self, code: str, message: str, **context: Any) -> None:
        super().__init__(message)
        self.code = code
        self.context: dict[str, Any] = context
