"""Whether a deferred target's manifest still matches what `dbt parse` would write for it.

With `defer.auto_compile`, SST runs dbt for the deferred target, usually production, on every
run that defers. On a large project that parse is most of the run, and it writes the same
manifest each time the project has not changed. After a parse SST writes `sst_stamp.json` beside
the manifest: a digest of everything the parse read. A later run whose digest is the same reads
the manifest as it is instead of parsing again.

The digest is over each dbt input file's path, size and modification time, not its content, so
a file touched without being changed makes the next run parse; it can only parse more often
than it needs to, never less. What it covers:

- every file under the directories `dbt_project.yml` names for models, macros, seeds,
  snapshots, analyses and tests, each at dbt's default when not named, and the installed
  packages under `packages-install-path`;
- `dbt_project.yml`, `packages.yml`, `dependencies.yml`, `package-lock.yml`, and the
  `profiles.yml` dbt is given;
- the target, the dbt command, every `DBT_` environment variable, and the dbt executable
  (`executable_key`, which carries SST's version).

A `var()` or `env_var()` the project reads from any other environment variable is not covered:
`defer.reuse_manifest: false` parses on every run.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import executable_key
from snowflake_semantic_tools.adapters.dbt.profiles import read_plain_yaml
from snowflake_semantic_tools.adapters.json_files import JsonFileError, read_json_file
from snowflake_semantic_tools.adapters.paths import UnsafeWrite, write_within

STAMP_NAME = "sst_stamp.json"
# Each dbt_project.yml key naming input directories, with dbt's default for it.
_INPUT_DIRS = {
    "model-paths": ("models",),
    "macro-paths": ("macros",),
    "seed-paths": ("seeds",),
    "snapshot-paths": ("snapshots",),
    "analysis-paths": ("analyses",),
    "test-paths": ("tests",),
}
_INPUT_FILES = ("dbt_project.yml", "packages.yml", "dependencies.yml", "package-lock.yml")


def parse_digest(project_dir: Path, profiles_dir: Path, target: str, command: str) -> str:
    """Return the digest of everything a `dbt parse` of `target` reads, as the module says."""
    entries = sorted(_inputs(project_dir, profiles_dir))
    environment = sorted((key, value) for key, value in os.environ.items() if key.startswith("DBT_"))
    document = {
        "target": target,
        "command": command,
        "dbt": executable_key(),
        "environment": environment,
        "files": entries,
    }
    return sha256(json.dumps(document, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def stamped_at(state_dir: Path, digest: str) -> datetime | None:
    """Return when the manifest in `state_dir` was stamped with `digest`; None when it was not.

    No manifest, no stamp, an unreadable stamp, or one with another digest is None: the
    manifest must be parsed again.
    """
    if not (state_dir / "manifest.json").is_file():
        return None
    try:
        stamp = read_json_file(state_dir / STAMP_NAME)
    except (OSError, JsonFileError):
        return None
    if not isinstance(stamp, Mapping) or stamp.get("digest") != digest:
        return None
    try:
        return datetime.fromisoformat(str(stamp.get("written_at")))
    except ValueError:
        return None


def write_stamp(state_dir: Path, digest: str, now: datetime | None = None) -> None:
    """Record that the manifest in `state_dir` is what a parse with `digest` wrote.

    A stamp that cannot be written is left out: the next run parses again.
    """
    written = (now or datetime.now(UTC)).isoformat()
    try:
        write_within(state_dir, state_dir / STAMP_NAME, json.dumps({"digest": digest, "written_at": written}) + "\n")
    except (OSError, UnsafeWrite):
        return


def _inputs(project_dir: Path, profiles_dir: Path) -> Iterator[tuple[str, int, int]]:
    """Yield each dbt input file as (path, size, modification time)."""
    root = project_dir.resolve()
    project = read_plain_yaml(root / "dbt_project.yml")
    directories: set[str] = {str(project.get("packages-install-path") or "dbt_packages")}
    for key, default in _INPUT_DIRS.items():
        declared = project.get(key)
        named = declared if isinstance(declared, list) and declared else list(default)
        directories.update(str(item) for item in named)
    files = [root / name for name in _INPUT_FILES]
    files.append(profiles_dir.resolve() / "profiles.yml")
    for directory in sorted(directories):
        base = root / directory
        if base.is_dir():
            files.extend(path for path in base.rglob("*") if path.is_file())
    for path in files:
        try:
            status = path.stat()
        except OSError:
            continue
        yield (str(path), status.st_size, status.st_mtime_ns)
