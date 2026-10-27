"""The live properties of a published object that its ownership marker does not prove, as a digest.

The marker in an object's comment names the definition SST wrote, and an edit made by hand
that keeps the comment keeps the marker: `ALTER PROCEDURE ... EXECUTE AS OWNER`, a search
service's new target lag, a hand-committed agent version, or a view re-created from its own
`GET_DDL`. So apply also records a digest of the properties SST publishes, read back once the
write succeeded, and plan reads the same properties again and compares the two.

Each property is read where Snowflake shows it:

| Object | Property | From |
|---|---|---|
| Agent | `specification` | the newest committed version's specification fingerprint |
| Cortex search service | `target_lag`, `warehouse` | SHOW CORTEX SEARCH SERVICES |
| Function | `secure` | SHOW FUNCTIONS |
| Procedure | `secure` | SHOW PROCEDURES |
| Procedure | `execute_as`, `body` | DESCRIBE PROCEDURE, unless the drift check is basic |
| Semantic view | `definition` | GET_DDL, unless the drift check is basic |

A digest is `<property>=<16 hex>` pairs, sorted by property and joined by `;`, each hex the
start of the SHA-256 of the property's normalized value. Only a property both digests carry is
compared, so a digest recorded before a property was read, or by a basic check, is never
drift for that property.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from hashlib import sha256

# The values of `plan.drift_check`: `full` reads every property, `basic` only what SHOW lists.
DRIFT_CHECK_FULL = "full"
DRIFT_CHECK_BASIC = "basic"
DRIFT_CHECKS = (DRIFT_CHECK_FULL, DRIFT_CHECK_BASIC)
# The object types whose digest reads SHOW's columns, DESCRIBE, or GET_DDL.
SHOWN_TYPES = frozenset(("CORTEX SEARCH SERVICE", "FUNCTION", "PROCEDURE"))
DESCRIBED_TYPES = frozenset(("PROCEDURE",))
DEFINED_TYPES = frozenset(("SEMANTIC VIEW",))
# The object types a digest is read for: those a generic apply publishes and plan replaces.
DIGESTED_TYPES = frozenset(("AGENT", "CORTEX SEARCH SERVICE", "FUNCTION", "PROCEDURE", "SEMANTIC VIEW"))
_DIGEST_HEX = 16
_PAIR = re.compile(r"([a-z_]+)=([0-9a-f]+)")


def drift_properties(
    object_type: str,
    shown: Mapping[str, str],
    *,
    described: Mapping[str, str] | None = None,
    definition: str | None = None,
    newest_spec: str | None = None,
) -> dict[str, str | None]:
    """Return the properties a digest covers for one object, None for one that was not read.

    Args:
        shown: The object's SHOW row, columns lowercased.
        described: What DESCRIBE reported for a procedure, properties lowercased; None unread.
        definition: A semantic view's GET_DDL text; None unread.
        newest_spec: The specification fingerprint of an agent's newest committed version.

    Returns:
        The properties by name, each value normalized so a read twice of an unchanged object
        gives the same text; empty for an object type no digest covers.
    """
    kind = " ".join(object_type.upper().split())
    if kind == "AGENT":
        return {"specification": newest_spec}
    if kind == "CORTEX SEARCH SERVICE":
        return {"target_lag": _word(shown.get("target_lag")), "warehouse": _word(shown.get("warehouse"))}
    if kind == "FUNCTION":
        return {"secure": _word(shown.get("is_secure"))}
    if kind == "PROCEDURE":
        found = described or {}
        return {
            "secure": _word(shown.get("is_secure")),
            "execute_as": _word(found.get("execute as")) if described is not None else None,
            "body": found.get("body") if described is not None else None,
        }
    if kind == "SEMANTIC VIEW":
        return {"definition": " ".join(definition.split()).rstrip(";").rstrip() if definition is not None else None}
    return {}


def live_digest(properties: Mapping[str, str | None]) -> str | None:
    """Digest the properties that were read; None when none was.

    Example:
        live_digest({"secure": "N", "body": None}) gives `secure=<16 hex>`.
    """
    pairs = sorted(
        (name, sha256(value.encode("utf-8")).hexdigest()[:_DIGEST_HEX])
        for name, value in properties.items()
        if value is not None
    )
    return ";".join(f"{name}={digest}" for name, digest in pairs) or None


def drifted_properties(recorded: str | None, live: str | None) -> tuple[str, ...]:
    """Name each property both digests carry with different values, in name order.

    Either digest unknown, as an entry recorded before SST read one, is no drift.
    """
    if recorded is None or live is None:
        return ()
    before, after = _pairs(recorded), _pairs(live)
    return tuple(name for name in sorted(before.keys() & after.keys()) if before[name] != after[name])


def _pairs(digest: str) -> dict[str, str]:
    """Read a digest's pairs; a part that is not one is skipped, so it compares as unread."""
    return {match.group(1): match.group(2) for part in digest.split(";") if (match := _PAIR.fullmatch(part))}


def _word(value: str | None) -> str | None:
    """Normalize a SHOW or DESCRIBE value: its words single-spaced and upper-cased; None for none."""
    return " ".join(value.split()).upper() if value is not None else None
