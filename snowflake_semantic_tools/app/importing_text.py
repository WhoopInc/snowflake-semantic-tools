"""Read what Snowflake shows of an object to import: SHOW and DESCRIBE text, and a plugin manifest.

Each reader returns an empty value for text it cannot read, so an object whose column is
missing or malformed is still imported, with what could be read.
"""

from __future__ import annotations

import json
from collections.abc import Mapping


def argument_types(arguments: str) -> tuple[str, ...]:
    """Return a routine's argument types from SHOW's `arguments`: `F(VARCHAR, NUMBER) RETURN VARCHAR`."""
    inside = arguments.partition(" RETURN ")[0].partition("(")[2].rpartition(")")[0]
    return tuple(part.split()[0].upper() for part in inside.split(",") if part.split())


def parse_signature(described: str, types: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    """Return a routine's arguments by name and type, from DESCRIBE's `signature`: `(ORDER_ID VARCHAR)`."""
    inside = described.strip()
    if inside.startswith("(") and inside.endswith(")"):
        inside = inside[1:-1]
    pairs = []
    for part in (piece.strip() for piece in inside.split(",")):
        words = part.split()
        if len(words) >= 2:
            pairs.append((words[0], words[1].upper()))
    return tuple(pairs) if len(pairs) == len(types) else ()


def parse_manifest(content: bytes | None) -> Mapping[str, object]:
    """Return a plugin version's `.cortex-plugin/plugin.json`, parsed; empty when it has none that reads."""
    if not content:
        return {}
    try:
        parsed = json.loads(content.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_profile(value: str | None) -> Mapping[str, object]:
    """Return SHOW AGENTS' `profile` JSON as a mapping; empty when it is unset or does not read."""
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_secure(value: str | None) -> bool | None:
    """Return SHOW AGENTS' `is_secure`; None when the role cannot see it."""
    if value is None or not value.strip():
        return None
    return value.strip().casefold() in ("true", "y", "yes")
