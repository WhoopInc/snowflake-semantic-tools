"""Read the baseline file: the committed record of warnings a project knows about.

Only reading lives here; writing it is `sst baseline`. The file is JSON, version 1, with a
mandatory `expires_on` and one entry per recorded diagnostic, keyed on its stable fingerprint.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import NoReturn

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry

BASELINE_FILE = Path(".sst") / "baseline.json"


def read_baseline(path: Path, name: str) -> Baseline:
    """Read the baseline file at `path`, which diagnostics call `name`.

    Raises:
        ProjectError: the file cannot be read, is not JSON, or is not a version 1 baseline with
            an `expires_on` date and a list of entries (SST-PRT009).

    Diagnostics:
        SST-PRT009: the baseline file cannot be read or does not hold a baseline; raised.
    """
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _refuse(name, str(exc))
    if not isinstance(document, dict) or document.get("version") != 1:
        _refuse(name, "it is not a version 1 baseline")
    expires_on = document.get("expires_on")
    entries = document.get("entries")
    if not isinstance(expires_on, str) or not isinstance(entries, list):
        _refuse(name, "it needs an expires_on date and a list of entries")
    parsed: list[BaselineEntry] = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("fingerprint"), str):
            _refuse(name, "every entry needs a fingerprint")
        parsed.append(
            BaselineEntry(
                fingerprint=entry["fingerprint"],
                code=str(entry.get("code") or ""),
                artifact=str(entry.get("artifact") or ""),
                file=str(entry.get("file") or ""),
                note=str(entry.get("note") or ""),
            )
        )
    return Baseline(name, expires_on, tuple(parsed))


def _refuse(name: str, detail: str) -> NoReturn:
    diagnostic = D("SST-PRT009", subject=f"config:{name}", path=name, detail=detail)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
