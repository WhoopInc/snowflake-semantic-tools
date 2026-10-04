"""Read and render the baseline file: the committed record of warnings a project knows about.

The file is JSON, version 1, with a mandatory `expires_on` and one entry per recorded diagnostic,
keyed on its stable fingerprint. `sst baseline` writes the text `baseline_text` renders: when and
by which SST it was first written, every renewal and its reason, and the entries, sorted so a
change to the file reviews as a diff of the entries it touched.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import NoReturn

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import JsonFileError, read_json_file
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry, Renewal

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
        document = read_json_file(path)
    except (OSError, JsonFileError) as exc:
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
    return Baseline(
        name,
        expires_on,
        tuple(parsed),
        generated_at=str(document.get("generated_at") or ""),
        generated_by=str(document.get("generated_by_sst") or ""),
        renewals=_renewals(document.get("renewals")),
    )


def _renewals(value: object) -> tuple[Renewal, ...]:
    """Read the renewal record; a malformed one is kept as far as it reads, never refused."""
    if not isinstance(value, list):
        return ()
    return tuple(
        Renewal(str(item.get("renewed_on") or ""), str(item.get("reason") or ""), str(item.get("expires_on") or ""))
        for item in value
        if isinstance(item, dict)
    )


def baseline_text(baseline: Baseline) -> str:
    """Render `baseline` as the file's JSON text, entries sorted by code, artifact and fingerprint."""
    entries = sorted(baseline.entries, key=lambda entry: (entry.code, entry.artifact, entry.fingerprint))
    document: dict[str, object] = {
        "version": 1,
        "generated_at": baseline.generated_at,
        "generated_by_sst": baseline.generated_by,
        "expires_on": baseline.expires_on,
    }
    if baseline.renewals:
        document["renewals"] = [
            {"renewed_on": item.renewed_on, "reason": item.reason, "expires_on": item.expires_on}
            for item in baseline.renewals
        ]
    document["entries"] = [
        {
            "fingerprint": entry.fingerprint,
            "code": entry.code,
            "artifact": entry.artifact,
            "file": entry.file,
            "note": entry.note,
        }
        for entry in entries
    ]
    return json.dumps(document, indent=2, ensure_ascii=False) + "\n"


def _refuse(name: str, detail: str) -> NoReturn:
    diagnostic = D("SST-PRT009", subject=f"config:{name}", path=name, detail=detail)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
