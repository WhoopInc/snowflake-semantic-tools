"""Reading JSON a user can hand SST, bounded so a crafted file is refused rather than a crash.

A state, plan, observation, baseline, lock, run log, MCP config, coverage catalog, or dbt manifest
is read here. A file larger than `MAX_JSON_BYTES`, or with arrays and objects nested deeper than
`MAX_JSON_DEPTH`, is refused like text that is not JSON: as `JsonFileError`, a `ValueError`, which
each caller reports with the code it already gives a malformed file of its kind. Nothing a reader
of the parsed value does then recurses deeper than the bound allows.
"""

from __future__ import annotations

import json
from pathlib import Path

# Far beyond any document SST or dbt writes: a dbt manifest is a few hundred MiB at most.
MAX_JSON_BYTES = 1 << 30
# Far beyond the nesting of any document SST or dbt writes, and far within Python's recursion
# limit for code that walks the value.
MAX_JSON_DEPTH = 200


class JsonFileError(ValueError):
    """JSON SST refuses to read: not UTF-8, not JSON, too large, or nested too deeply.

    The message is the reason alone, worded to follow "<path>: " or "could not read <path>: ": the
    decoder's own words for text that is not UTF-8 JSON, else which bound the file exceeds.
    """


def read_bounded(path: Path, *, max_bytes: int | None = None) -> bytes:
    """Return the bytes of the file at `path`, refusing one larger than `max_bytes`.

    `max_bytes` defaults to `MAX_JSON_BYTES` as it is when called.

    Raises:
        FileNotFoundError: there is no file at `path`.
        OSError: the file cannot be opened or read.
        JsonFileError: the file is larger than `max_bytes`.
    """
    limit = MAX_JSON_BYTES if max_bytes is None else max_bytes
    with path.open("rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise JsonFileError(f"it is larger than the {limit} bytes SST reads")
    return raw


def read_json_file(path: Path, *, max_bytes: int | None = None, max_depth: int | None = None) -> object:
    """Read and parse the JSON file at `path`, refusing one that is too large or nested too deeply.

    The bounds default to `MAX_JSON_BYTES` and `MAX_JSON_DEPTH` as they are when called.

    Raises:
        FileNotFoundError: there is no file at `path`.
        OSError: the file cannot be opened or read.
        JsonFileError: the file is larger than `max_bytes`, is not UTF-8 JSON, or nests arrays
            and objects deeper than `max_depth`.
    """
    return parse_json(read_bounded(path, max_bytes=max_bytes), max_depth=max_depth)


def parse_json(raw: bytes | str, *, max_depth: int | None = None) -> object:
    """Parse UTF-8 JSON text, refusing a document nested deeper than `max_depth`.

    `max_depth` defaults to `MAX_JSON_DEPTH` as it is when called.

    Raises:
        JsonFileError: the text is not UTF-8 JSON, or nests arrays and objects deeper than
            `max_depth`.
    """
    limit = MAX_JSON_DEPTH if max_depth is None else max_depth
    try:
        value = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except RecursionError as exc:
        raise JsonFileError(f"it nests arrays or objects deeper than {limit} levels") from exc
    except ValueError as exc:
        # JSONDecodeError, and UnicodeDecodeError for bytes that are not UTF-8, in their own words.
        raise JsonFileError(str(exc)) from exc
    if _deeper_than(value, limit):
        raise JsonFileError(f"it nests arrays or objects deeper than {limit} levels")
    return value


def _deeper_than(value: object, limit: int) -> bool:
    """Whether arrays and objects in `value` nest more than `limit` levels, found without recursing."""
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        node, depth = stack.pop()
        children = node.values() if isinstance(node, dict) else node if isinstance(node, list) else None
        if children is None:
            continue
        if depth > limit:
            return True
        stack.extend((child, depth + 1) for child in children if isinstance(child, (dict, list)))
    return False
