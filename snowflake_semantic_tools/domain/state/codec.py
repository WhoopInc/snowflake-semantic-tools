"""Readers and writers for the shapes SST's persisted JSON documents share.

Manifests, local state, Snowflake state rows and saved plans all carry component
fingerprints as a JSON object and physical resources as a list of objects. These
functions hold the one in-memory form of each: fingerprints as sorted `(key, value)`
text pairs, resources as `(object_type, qualified_name)` pairs. A writer produces
exactly what its reader accepts, so a document round-trips byte for byte through
`canonical_json`.

Readers raise `ValueError` with the caller's own message, so every document keeps
the error text it has always reported; the store that read the file maps the
error to its diagnostic.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def optional_object(value: object, message: str) -> dict[str, Any]:
    """Return a JSON object with text keys, reading an absent value (None) as empty.

    Raises:
        ValueError: `message`, when the value is present and not an object.
    """
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(message)
    return {str(key): item for key, item in value.items()}


def pairs_from_json(value: Mapping[Any, object]) -> tuple[tuple[str, str], ...]:
    """Return a JSON object as sorted `(key, value)` text pairs, the form fingerprints take in memory."""
    return tuple(sorted((str(key), str(item)) for key, item in value.items()))


def pairs_to_json(pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Return `(key, value)` pairs as the JSON object `pairs_from_json` reads back."""
    return dict(pairs)


def resources_from_json(values: Iterable[object], message: str) -> tuple[tuple[str, str], ...]:
    """Return a list of physical-resource objects as `(object_type, qualified_name)` pairs, in order.

    Raises:
        ValueError: `message`, at the first entry that is not an object.
        KeyError: when an entry lacks `object_type` or `qualified_name`.
    """
    parsed: list[tuple[str, str]] = []
    for value in values:
        if not isinstance(value, dict):
            raise ValueError(message)
        parsed.append((str(value["object_type"]), str(value["qualified_name"])))
    return tuple(parsed)


def resources_to_json(resources: Iterable[tuple[str, str]]) -> list[dict[str, str]]:
    """Return `(object_type, qualified_name)` pairs as the list `resources_from_json` reads back."""
    return [{"object_type": object_type, "qualified_name": qualified_name} for object_type, qualified_name in resources]
