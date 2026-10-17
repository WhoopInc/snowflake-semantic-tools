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

import yaml

from tests.helpers.network_guard import guarded_environment
from tests.helpers.reference_project import DBT_MANIFEST, REPO_ROOT, project_copy, set_column_meta

OVERLAYS = REPO_ROOT / "tests" / "fixtures" / "negative_overlays"
_MANIFEST_PATCH = ".manifest.json"
# A change that spans files, or edits one in place: `remove` deletes files, `files` writes them,
# `edits` replaces text that must occur exactly once, and `manifest` patches one column's
# `meta.sst` as a `.manifest.json` overlay does. Text, not a YAML round trip, so the commented
# fixture keeps its lines and a diagnostic's line number still means what the file says.
_TREE = ".tree.yml"
_TREE_KEYS = frozenset({"remove", "files", "edits", "manifest"})
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
    """Run `sst` with `args` from the repository root and wait for it to finish.

    Outside a live test the child holds the network guard (`network_guard.guarded_environment`),
    whose directory stays first on `PYTHONPATH`: the repository root is added only when the guard
    has not put it there already.
    """
    env = guarded_environment(os.environ if environ is None else environ)
    if str(REPO_ROOT) not in env.get("PYTHONPATH", "").split(os.pathsep):
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


def expected_cases() -> dict[str, dict[str, Any]]:
    """The negative corpus's expectations, by overlay file name."""
    value: dict[str, dict[str, Any]] = json.loads((OVERLAYS / "expected.json").read_text(encoding="utf-8"))
    return value


def overlay_files() -> list[str]:
    """Every overlay in the corpus directory, in name order."""
    return sorted(path.name for path in OVERLAYS.iterdir() if path.name != "expected.json")


def _tree(overlay: str) -> dict[str, Any]:
    tree: dict[str, Any] = yaml.safe_load((OVERLAYS / overlay).read_text(encoding="utf-8"))
    assert tree and set(tree) <= _TREE_KEYS, f"{overlay} may hold only {sorted(_TREE_KEYS)}"
    return tree


def overlay_paths(overlay: str, into: str) -> set[str]:
    """The project-relative files `overlay` writes, edits or removes; a manifest patch writes none."""
    if overlay.endswith(_TREE):
        tree = _tree(overlay)
        return {*tree.get("remove", ()), *tree.get("files", {}), *tree.get("edits", {})}
    return set() if overlay.endswith(_MANIFEST_PATCH) else {f"{into}/{overlay}"}


def _lay_tree(project: Path, overlay: str) -> dict[str, Any] | None:
    """Remove, write and edit the tree's files; return its manifest patch, None for none."""
    tree = _tree(overlay)
    for relative in tree.get("remove", ()):
        (project / relative).unlink()
    for relative, text in tree.get("files", {}).items():
        path = project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    for relative, edits in tree.get("edits", {}).items():
        path = project / relative
        text = path.read_text(encoding="utf-8")
        for edit in edits:
            assert text.count(edit["old"]) == 1, f"{overlay}: {edit['old']!r} is not in {relative} once"
            text = text.replace(edit["old"], edit["new"])
        path.write_text(text, encoding="utf-8")
    patch: dict[str, Any] | None = tree.get("manifest")
    return patch


def overlaid(root: Path, overlay: str, into: str) -> tuple[Path, Path]:
    """Lay `overlay` over a reference copy; return the project and the dbt manifest to read.

    A `.manifest.json` overlay patches one column's `meta.sst` in a copy of the manifest, which is
    where a dbt model's YAML arrives once dbt has parsed it; a `.tree.yml` overlay removes, writes
    and edits files across the project, and may patch the manifest too; any other overlay is
    copied into `into` under the project.
    """
    project = project_copy(root, offline=False)
    if overlay.endswith(_TREE):
        tree_patch = _lay_tree(project, overlay)
        return project, DBT_MANIFEST if tree_patch is None else _patched_manifest(root, tree_patch)
    if not overlay.endswith(_MANIFEST_PATCH):
        shutil.copy(OVERLAYS / overlay, project / into / overlay)
        return project, DBT_MANIFEST
    return project, _patched_manifest(root, json.loads((OVERLAYS / overlay).read_text(encoding="utf-8")))


def _patched_manifest(root: Path, patch: dict[str, Any]) -> Path:
    """Write a copy of the reference manifest with one column's `meta.sst` patched; return its path."""
    document = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    set_column_meta(document, patch["model"], patch["column"], **patch["meta"])
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps(document), encoding="utf-8")
    return manifest


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
