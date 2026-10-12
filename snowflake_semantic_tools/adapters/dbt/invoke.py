"""Run `dbt parse` for a project, and report every way that can fail before SST reads the manifest.

dbt runs as an argument list through a `DbtRunner`, never through a shell. The default runner
is `subprocess_runner`; tests pass a fake. In order, `parse_project`:

1. refuses to start while `packages.yml` declares packages that are not installed (SST-DBT026);
2. runs `dbt --version`, refusing a dbt that cannot be run (SST-DBT027), warning when the
   installation cannot be identified (SST-DBT020), and refusing `defer.auto_compile` under
   the dbt Cloud CLI, which cannot parse a target other than its default (SST-DBT021);
3. runs `dbt parse`, refusing a non-zero exit (SST-DBT028) after passing dbt's own output on,
   and a zero exit that wrote no manifest where dbt_project.yml says it would (SST-DBT029).

Starting dbt only to ask what it is costs about two seconds, so with an `installation_cache`
the answer to step 2 is kept, keyed by the `dbt` executable on `PATH` -- its path, the file it
resolves to, that file's size and modification time -- and by SST's version. Installing,
upgrading or switching dbt rewrites or replaces that file, so the next run asks again. Only an
identified installation is kept: one that fails or names no installation is asked every run.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import JsonFileError, read_json_file
from snowflake_semantic_tools.adapters.paths import UnsafeWrite, write_within
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.state import SST_VERSION

# The installation kinds `installation_type` names, the only answers the cache keeps.
_KINDS = frozenset({"core", "cloud"})


@dataclass(frozen=True, slots=True)
class CompletedRun:
    """What one dbt process did: its exit status and what it printed."""

    returncode: int
    stdout: str
    stderr: str


DbtRunner = Callable[[Sequence[str], Path], CompletedRun]
"""Run one argument list in a directory; raise `OSError` when the executable cannot be started."""


def subprocess_runner(argv: Sequence[str], cwd: Path) -> CompletedRun:
    """Run `argv` with no shell in `cwd`, capturing its output; `RUN_TIMINGS` counts the process.

    Raises:
        OSError: The executable is not on `PATH` or cannot be started.
    """
    with RUN_TIMINGS.dbt_process():
        completed = subprocess.run(list(argv), cwd=cwd, capture_output=True, text=True, check=False)
    return CompletedRun(completed.returncode, completed.stdout or "", completed.stderr or "")


def _refuse(diagnostic: Diagnostic, cause: Exception | None = None) -> ProjectError:
    error = ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    error.__cause__ = cause
    return error


def _yaml_mapping(path: Path) -> Mapping[str, Any]:
    """Read a dbt-owned YAML file as plain YAML; an absent, unreadable or non-mapping one is empty."""
    try:
        value = bounded_yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return value if isinstance(value, dict) else {}


def _package_dir_name(entry: Mapping[str, Any]) -> str | None:
    """The directory `dbt deps` installs one `packages.yml` entry under, by dbt's naming."""
    for key in ("package", "git", "local", "tarball"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            name = value.strip().rstrip("/").rsplit("/", 1)[-1]
            return name.removesuffix(".git").removesuffix(".tar.gz")
    return None


def missing_packages(project_dir: Path) -> int:
    """Count the `packages.yml` entries with no directory under dbt_project.yml's packages install path."""
    declared = _yaml_mapping(project_dir / "packages.yml").get("packages")
    install_path = str(_yaml_mapping(project_dir / "dbt_project.yml").get("packages-install-path") or "dbt_packages")
    installed = project_dir / install_path
    names = (
        [_package_dir_name(entry) for entry in declared or [] if isinstance(entry, dict)]
        if isinstance(declared, list)
        else []
    )
    return sum(1 for name in names if name and not (installed / name).is_dir())


def installation_type(output: str) -> str | None:
    """Identify a dbt installation from what `dbt --version` printed: `core`, `cloud`, or None."""
    text = output.casefold()
    if "dbt cloud cli" in text or "cloud cli -" in text:
        return "cloud"
    if "dbt-core" in text or ("core:" in text and "installed:" in text):
        return "core"
    return None


def _run(runner: DbtRunner, argv: Sequence[str], cwd: Path) -> CompletedRun:
    """Run dbt, refusing one that cannot be started at all.

    Diagnostics:
        SST-DBT027: the executable cannot be started; raised.
    """
    try:
        return runner(argv, cwd)
    except OSError as exc:
        raise _refuse(D("SST-DBT027", detail=f"{argv[0]}: {exc.strerror or exc}"), exc) from exc


def _installation(
    runner: DbtRunner, project_dir: Path, auto_compile: bool, cache: Path | None = None
) -> tuple[Diagnostic, ...]:
    """Ask dbt what it is, unless `cache` holds the answer for this dbt, and refuse what it cannot do.

    A `dbt --version` that runs and exits non-zero names no installation either: dbt still ran,
    so it is the `dbt parse` that follows which reports a broken install (SST-DBT028).

    Diagnostics:
        SST-DBT027: `dbt --version` cannot be started; raised.
        SST-DBT021: `defer.auto_compile` is set under the dbt Cloud CLI; raised.
        SST-DBT020: the output names no installation SST knows, or `dbt --version` exits
            non-zero; returned.
    """
    key = executable_key() if cache is not None else None
    kind = _cached_kind(cache, key)
    if kind is None:
        completed = _run(runner, ("dbt", "--version"), project_dir)
        output = f"{completed.stdout}\n{completed.stderr}".strip()
        if completed.returncode != 0:
            first = output.splitlines()[0] if output else "no output"
            return (D("SST-DBT020", found=f"{first!r} (exit {completed.returncode})"),)
        kind = installation_type(output)
        if kind is None:
            first = output.splitlines()[0] if output else "no output"
            return (D("SST-DBT020", found=repr(first)),)
        _store_kind(project_dir, cache, key, kind)
    if kind == "cloud" and auto_compile:
        raise _refuse(D("SST-DBT021", origin=Origin("sst_config.yml"), found="the dbt Cloud CLI"))
    return ()


def executable_key() -> dict[str, object] | None:
    """Identify the `dbt` a run would start: what the cache keys an answer by; None without one.

    The key is the executable `PATH` finds, the file it resolves to, that file's size and
    modification time, and SST's version, so a reinstalled, upgraded or other dbt, or another
    SST, never reads an answer kept for a different one.
    """
    found = shutil.which("dbt")
    if found is None:
        return None
    resolved = os.path.realpath(found)
    try:
        status = os.stat(resolved)
    except OSError:
        return None
    return {
        "executable": found,
        "resolved": resolved,
        "size": status.st_size,
        "mtime_ns": status.st_mtime_ns,
        "sst": SST_VERSION,
    }


def _cached_kind(cache: Path | None, key: dict[str, object] | None) -> str | None:
    """Return the installation `cache` holds for `key`; None when it holds none, another, or is unreadable."""
    if cache is None or key is None:
        return None
    try:
        held = read_json_file(cache)
    except (OSError, JsonFileError):
        return None
    if not isinstance(held, dict) or held.get("key") != key:
        return None
    kind = held.get("kind")
    return kind if isinstance(kind, str) and kind in _KINDS else None


def _store_kind(project_dir: Path, cache: Path | None, key: dict[str, object] | None, kind: str) -> None:
    """Keep `kind` in `cache` for `key`; a cache that cannot be written is only not kept."""
    if cache is None or key is None:
        return
    try:
        write_within(project_dir, cache, json.dumps({"key": key, "kind": kind}, sort_keys=True) + "\n")
    except (OSError, UnsafeWrite):
        return


def parse_project(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path,
    *,
    runner: DbtRunner = subprocess_runner,
    auto_compile: bool = False,
    echo: Callable[[str], object] = sys.stderr.write,
    profiles_dir: Path | None = None,
    target_path: Path | None = None,
    command: str = "parse",
    installation_cache: Path | None = None,
) -> tuple[Diagnostic, ...]:
    """Run `dbt parse` (or `dbt.command`) for the target named, with the profiles SST resolved.

    Args:
        manifest_path: Where dbt writes the manifest: under dbt_project.yml's target path, or
            under `target_path` when it is given.
        profiles_dir: The directory of the `profiles.yml` SST resolves targets against; the
            project directory when None.
        auto_compile: `defer.auto_compile` as `sst_config.yml` sets it.
        echo: Where dbt's own output goes when it fails, so the user reads dbt's words.
        target_path: The directory dbt writes its artifacts to instead of its own target path,
            as `--target-path` asks; a deferred target's manifest is written there.
        command: The dbt command that writes the manifest, `parse` or `compile` (`dbt.command`).
        installation_cache: The file that keeps what `dbt --version` answered, inside the
            project, as the module describes; None asks dbt every run.

    Returns:
        The warnings the run found; anything worse is raised.

    Raises:
        ProjectError: As the diagnostics say.

    Diagnostics:
        SST-DBT026: packages are declared and not installed; raised before dbt runs.
        SST-DBT027, SST-DBT021, SST-DBT020: as `_installation` reports them.
        SST-DBT028: `dbt parse` exits non-zero; raised.
        SST-DBT029: `dbt parse` exits zero and the manifest is not there; raised.
    """
    missing = missing_packages(project_dir)
    if missing:
        raise _refuse(D("SST-DBT026", origin=Origin("packages.yml"), count=missing))
    warnings = _installation(runner, project_dir, auto_compile, installation_cache)
    # Absolute, because dbt runs inside the project and would resolve a relative path a second time.
    root = project_dir.resolve()
    arguments = ["dbt", command, "--project-dir", str(root), "--profiles-dir", str((profiles_dir or root).resolve())]
    if target_name:
        arguments.extend(("--target", target_name))
    if target_path is not None:
        arguments.extend(("--target-path", str(target_path.resolve())))
    completed = _run(runner, arguments, root)
    if completed.returncode != 0:
        echo(completed.stdout + completed.stderr)
        raise _refuse(D("SST-DBT028", found=completed.returncode))
    if not manifest_path.is_file():
        try:
            shown = manifest_path.relative_to(project_dir).as_posix()
        except ValueError:
            shown = str(manifest_path)
        raise _refuse(D("SST-DBT029", path=shown))
    return warnings
