"""The local run log: one line per Snowflake refusal a run reported, under the build directory.

Every command appends the `SST-SNO` diagnostics it reports to `target/sst/run_log.jsonl`, and
`sst debug --snowflake-signatures` reads it back as a rate: of the refusals SST saw, how many it
recognised, and how many it reported as SST-SNO001 because no signature matched. That rate is
the size of the failure surface SST does not yet classify.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

from snowflake_semantic_tools.adapters.paths import append_within
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import fragile_signatures

RUN_LOG = "run_log.jsonl"
_UNMATCHED = "SST-SNO001"


def append_run_log(root: Path, build_dir: Path, diagnostics: Iterable[Diagnostic], *, command: str) -> None:
    """Append one line per `SST-SNO` diagnostic to the run log, inside `root`; none when there is none."""
    lines = [
        json.dumps({"command": command, "code": item.code}, sort_keys=True)
        for item in diagnostics
        if item.code.startswith("SST-SNO")
    ]
    if not lines:
        return
    append_within(root, build_dir / RUN_LOG, "".join(f"{line}\n" for line in lines))


def signature_report(build_dir: Path) -> dict[str, object]:
    """Return how many logged refusals matched a signature, how many did not, and the unmatched rate.

    A line that does not parse is skipped. `fragile` lists the signatures matched by message text
    alone, with no SQLSTATE or error number: each is a string matcher that a reworded Snowflake
    error would silently miss, and the rate above is how often one already has.
    """
    path = build_dir / RUN_LOG
    codes: list[str] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict) and isinstance(entry.get("code"), str):
                codes.append(entry["code"])
    unmatched = codes.count(_UNMATCHED)
    return {
        "log": str(path),
        "matched": len(codes) - unmatched,
        "unmatched": unmatched,
        "sno001_rate": round(unmatched / len(codes), 4) if codes else 0.0,
        "fragile": [
            {"code": row.code, "pattern": row.pattern.pattern if row.pattern else None, "kind": row.kind.value}
            for row in fragile_signatures()
        ],
    }
