"""Turn a manifest `dbt parse` wrote into a committed fixture under `tests/fixtures/dbt_manifests`.

A manifest carries the run that wrote it: the absolute project path, an invocation id,
timestamps, the environment dbt passes through, and the macros of dbt, its adapter and every
package. `scrub` replaces the first four with fixed values and drops `macros`, which SST never
reads (docs/design/dbt-compatibility.md lists what it does read), so a fixture says only what
every capture of the same project with the same dbt says. `fixture_path` is where a manifest
of one schema version and one dbt release lives.

Capture, in an environment with the dbt release and its Snowflake adapter installed, from a copy
of `tests/fixtures/reference_project` with its packages installed:

    dbt parse --no-partial-parse --profiles-dir <dir with a dummy profile> --target dev
    python -m tests.helpers.manifest_capture <copy>/target/manifest.json <copy>

The dummy profile names the `dev` target's database, schema and warehouse, and a placeholder
account and user; parsing connects to nothing.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.dbt.manifest import schema_version_number

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "dbt_manifests"
# Where every capture's project directory reads, whatever directory the capture ran in.
PROJECT_ROOT = "/reference_project"
_EPOCH = "1970-01-01T00:00:00Z"
_RUN_METADATA = {
    "generated_at": _EPOCH,
    "invocation_started_at": _EPOCH,
    "run_started_at": _EPOCH,
    "invocation_id": "00000000-0000-0000-0000-000000000000",
    "env": {},
    "user_id": None,
}


def _replaced(value: Any, roots: tuple[str, ...]) -> Any:
    """`value` with every capture root in its strings replaced, and every `created_at` zeroed."""
    if isinstance(value, dict):
        return {
            key: 0.0 if key == "created_at" and isinstance(item, int | float) else _replaced(item, roots)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_replaced(item, roots) for item in value]
    if isinstance(value, str):
        for root in roots:
            value = value.replace(root, PROJECT_ROOT)
    return value


def scrub(document: Mapping[str, Any], project_dir: Path) -> dict[str, Any]:
    """Return `document` without what one run, one machine or one directory put in it.

    `project_dir` is the directory dbt parsed, matched both as given and resolved, since dbt
    may write either spelling (a temporary directory often resolves through a symlink).
    """
    roots = tuple(sorted({str(project_dir), str(project_dir.resolve())}, key=len, reverse=True))
    scrubbed: dict[str, Any] = _replaced(dict(document), roots)
    metadata = dict(scrubbed.get("metadata") or {})
    metadata.update({key: value for key, value in _RUN_METADATA.items() if key in metadata})
    scrubbed["metadata"] = metadata
    if "macros" in scrubbed:
        scrubbed["macros"] = {}
    return scrubbed


def fixture_path(document: Mapping[str, Any]) -> Path:
    """Where a manifest of this schema version and dbt release is committed."""
    metadata = document.get("metadata") or {}
    number = schema_version_number(metadata.get("dbt_schema_version"))
    if number is None:
        raise ValueError(f"the manifest names no schema version: {metadata.get('dbt_schema_version')!r}")
    return FIXTURES / f"v{number}" / f"dbt-{metadata['dbt_version']}" / "manifest.json"


def write_fixture(document: Mapping[str, Any], path: Path) -> None:
    """Write a scrubbed manifest in the stable form the fixtures are committed in."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    """Scrub the manifest at `argv[0]`, parsed in project directory `argv[1]`, into its fixture."""
    if len(argv) != 2:
        print("usage: python -m tests.helpers.manifest_capture <manifest.json> <project dir>", file=sys.stderr)
        return 2
    document = scrub(json.loads(Path(argv[0]).read_text(encoding="utf-8")), Path(argv[1]))
    path = fixture_path(document)
    write_fixture(document, path)
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
