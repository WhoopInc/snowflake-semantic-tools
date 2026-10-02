"""`sst` as a user runs it, for the end-to-end layer: a subprocess, its exit code, and its envelope.

`run_sst` starts `python -m snowflake_semantic_tools.cli.main` from the repository root, so the
checkout under test is the one that runs, and keeps stdout and stderr apart: `--output json` owns
stdout, and an end-to-end test treats anything on stderr as a failure. `overlaid` lays one negative
overlay over a fresh copy of the reference project, the way a user's one bad file sits among good
ones.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.helpers.cli_projects import DBT_MANIFEST, FIXTURE, REPO_ROOT
from tests.helpers.semantic_projects import set_column_meta

OVERLAYS = REPO_ROOT / "tests" / "fixtures" / "negative_overlays"
_MANIFEST_PATCH = ".manifest.json"
# One diagnostic as the corpus counts it: the code, its declared severity, and the file it names.
Key = tuple[str, str, str | None]


@dataclass(frozen=True, slots=True)
class SstRun:
    """One finished `sst` invocation."""

    exit_code: int
    stdout: str
    stderr: str

    @property
    def envelope(self) -> dict[str, Any]:
        """The `--output json` envelope on stdout."""
        value: dict[str, Any] = json.loads(self.stdout)
        return value

    def blocking(self) -> Counter[Key]:
        """Every diagnostic above info, keyed by code, declared severity and file."""
        return Counter(
            (item["code"], item["declared_severity"], (item["location"] or {}).get("file"))
            for item in self.envelope["diagnostics"]
            if item["severity"] != "info"
        )


def run_sst(*args: str, environ: Mapping[str, str] | None = None, timeout: float = 600) -> SstRun:
    """Run `sst` with `args` from the repository root and wait for it to finish."""
    env = dict(os.environ if environ is None else environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(REPO_ROOT), env.get("PYTHONPATH"))))
    done = subprocess.run(
        [sys.executable, "-m", "snowflake_semantic_tools.cli.main", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return SstRun(done.returncode, done.stdout, done.stderr)


def reference_copy(root: Path) -> Path:
    """A fresh copy of the reference project under `root`, without a compiled target."""
    project = root / "project"
    shutil.copytree(FIXTURE, project, ignore=shutil.ignore_patterns("target"))
    return project


def expected_cases() -> dict[str, dict[str, Any]]:
    """The negative corpus's expectations, by overlay file name."""
    value: dict[str, dict[str, Any]] = json.loads((OVERLAYS / "expected.json").read_text(encoding="utf-8"))
    return value


def overlay_files() -> list[str]:
    """Every overlay in the corpus directory, in name order."""
    return sorted(path.name for path in OVERLAYS.iterdir() if path.name != "expected.json")


def overlaid(root: Path, overlay: str, into: str) -> tuple[Path, Path]:
    """Lay `overlay` over a reference copy; return the project and the dbt manifest to read.

    A `.manifest.json` overlay patches one column's `meta.sst` in a copy of the manifest, which is
    where a dbt model's YAML arrives once dbt has parsed it; any other overlay is copied into
    `into` under the project.
    """
    project = reference_copy(root)
    if not overlay.endswith(_MANIFEST_PATCH):
        shutil.copy(OVERLAYS / overlay, project / into / overlay)
        return project, DBT_MANIFEST
    patch = json.loads((OVERLAYS / overlay).read_text(encoding="utf-8"))
    document = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    set_column_meta(document, patch["model"], patch["column"], **patch["meta"])
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps(document), encoding="utf-8")
    return project, manifest


def strict_validate(project: Path, manifest: Path) -> SstRun:
    """`sst validate --strict` over `project`, offline, with the JSON envelope on stdout."""
    return run_sst(
        "--output",
        "json",
        "validate",
        "--project-dir",
        str(project),
        "--manifest",
        str(manifest),
        "--strict",
        "--no-snowflake-syntax-check",
    )
