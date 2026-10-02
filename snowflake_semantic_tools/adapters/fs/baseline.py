"""The project's diagnostic baseline: `.sst/baseline.json`, read and never written."""

from __future__ import annotations

import json
from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError

BASELINE_FILE = Path(".sst") / "baseline.json"


def read_baseline(project_dir: Path) -> tuple[str, ...]:
    """Return the fingerprint of each entry in the project's baseline; empty when there is none.

    Raises:
        ProjectError: the file is not JSON, or is not an object whose `entries` is a list of
            objects each with a string `fingerprint`.
    """
    path = project_dir / BASELINE_FILE
    if not path.is_file():
        return ()
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProjectError(f"cannot read the baseline {BASELINE_FILE}: {exc}") from exc
    entries = document.get("entries") if isinstance(document, dict) else None
    if not isinstance(entries, list) or not all(
        isinstance(entry, dict) and isinstance(entry.get("fingerprint"), str) for entry in entries
    ):
        raise ProjectError(f"{BASELINE_FILE} must hold an `entries` list of objects with a string `fingerprint`")
    return tuple(entry["fingerprint"] for entry in entries)
