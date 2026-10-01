"""What `sst` prints: the one JSON envelope per run, diagnostics as JSON, and diagnostics as text.

`SstGroup.main` calls `start_invocation` before parsing, so every envelope -- a command's,
a usage error's, or an interrupted run's -- reports the same command line and start time.
Human output renders each diagnostic with the domain's `render_diagnostic`, on stderr.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import NoReturn

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, INTERRUPTED, OK
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import (
    ERROR_REGISTRY,
    Diagnostic,
    DiagnosticBag,
    Severity,
    render_diagnostic,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

_INVOCATION: dict[str, object] = {}
_ARTIFACT_SUBJECTS = frozenset(SEMANTIC_REGISTRY.artifacts) | {"profile"}


def start_invocation(argv: list[str]) -> None:
    """Record the command line and the time this run started, for every envelope it prints."""
    _INVOCATION.clear()
    _INVOCATION.update(
        {
            "argv": argv,
            "started_at": SystemClock().now_iso(),
            "started_monotonic": time.monotonic(),
        }
    )


def diagnostic_json(value: Diagnostic) -> dict[str, object]:
    """Return one diagnostic as the envelope lists it, with its registered severity and suggestion.

    A subject that names an artifact type, or `profile`, is reported as `artifact`; any
    other `<type>:<name>` subject is a `member` of one.
    """
    origin = getattr(value, "origin", None)
    spec = ERROR_REGISTRY[value.code]
    subject_parts = split_artifact_key(value.subject) if value.subject and ":" in value.subject else None
    return {
        "code": value.code,
        "severity": value.severity.name.lower(),
        "declared_severity": spec.severity.name.lower(),
        "promoted_from": (spec.severity.name.lower() if value.severity is not spec.severity else None),
        "baselined": False,
        "message": value.message,
        "suggestion": spec.suggestion,
        "phase": value.phase,
        "help_url": value.help_url,
        "fingerprint": value.fingerprint[:16],
        "params": dict(value.context),
        "artifact": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] in _ARTIFACT_SUBJECTS
            else None
        ),
        "member": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] not in _ARTIFACT_SUBJECTS
            else None
        ),
        "location": (
            {
                "file": origin.file,
                "line": origin.line,
                "column": origin.col,
                "end_line": None,
                "end_column": None,
            }
            if origin is not None
            else None
        ),
        "related": [{"file": item.file, "line": item.line, "column": item.col} for item in value.related],
        "internal_detail": None,
        "caused_by": value.caused_by,
    }


def json_envelope(
    command: str,
    diagnostics: DiagnosticBag,
    *,
    exit_code: int | None = None,
    status: str | None = None,
    promoted: int = 0,
    data: object | None = None,
) -> dict[str, object]:
    """Build the one JSON document `--output json` prints for a run of `command`.

    Without `exit_code`, the run exits 1 when a diagnostic is an error and 0 otherwise;
    without `status`, it follows the exit code: `ok`, `changes` for 2, else `error`.
    """
    errors = diagnostics.count(Severity.ERROR)
    warnings = diagnostics.count(Severity.WARNING)
    info = diagnostics.count(Severity.INFO)
    resolved_exit = exit_code if exit_code is not None else ERROR if errors else OK
    return {
        "tool": "sst",
        "sst_version": VERSION,
        "schema_version": 2,
        "command": command,
        "status": status
        or ("error" if resolved_exit not in (OK, CHANGES) else "changes" if resolved_exit == CHANGES else "ok"),
        "exit_code": resolved_exit,
        "invocation": _invocation(command),
        "diagnostics": [diagnostic_json(diagnostic) for diagnostic in diagnostics],
        "summary": {
            "error": errors,
            "warning": warnings,
            "info": info,
            "promoted": promoted,
            "suppressed_cascade": sum(
                diagnostic.caused_by is not None and diagnostic.severity is Severity.INFO for diagnostic in diagnostics
            ),
            "baselined": 0,
        },
        "data": data if data is not None else {},
    }


def _invocation(command: str) -> dict[str, object]:
    """Return the envelope's `invocation`: the command line, target, project, config file, and timing."""
    project_dir = _invocation_option("--project-dir")
    target = _invocation_option("--target")
    started_monotonic = _INVOCATION.get("started_monotonic")
    raw_argv = _INVOCATION.get("argv")
    duration = max(time.monotonic() - started_monotonic, 0.0) if isinstance(started_monotonic, float) else 0.0
    project = Path(project_dir or ".")
    return {
        "argv": [str(value) for value in raw_argv] if isinstance(raw_argv, list) else ["sst", command],
        "target": target,
        "project_dir": str(project.resolve()),
        "config_file": (
            str((project / "sst_config.yml").resolve()) if (project / "sst_config.yml").is_file() else None
        ),
        "started_at": _INVOCATION.get("started_at"),
        "duration_s": round(duration, 6),
    }


def _invocation_option(name: str) -> str | None:
    """Return the value given for option `name` on the recorded command line, as `--x v` or `--x=v`."""
    argv = _INVOCATION.get("argv", ())
    if not isinstance(argv, list):
        return None
    for index, value in enumerate(argv):
        if value == name and index + 1 < len(argv):
            return str(argv[index + 1])
        if isinstance(value, str) and value.startswith(f"{name}="):
            return value.split("=", 1)[1]
    return None


def print_envelope(envelope: dict[str, object]) -> None:
    """Print `envelope` on stdout as compact JSON with sorted keys, the one form `sst` emits."""
    click.echo(json.dumps(envelope, sort_keys=True, separators=(",", ":")))


def emit_json(envelope: dict[str, object], exit_code: int) -> NoReturn:
    """Print `envelope`, and exit with `exit_code`."""
    print_envelope(envelope)
    raise click.exceptions.Exit(exit_code)


def interrupted(command: str, output: str, cause: BaseException) -> NoReturn:
    """End a run the user interrupted or declined to confirm with exit 130, never an internal error."""
    if output == "json":
        print_envelope(
            json_envelope(
                command, DiagnosticBag(), exit_code=INTERRUPTED, status="error", data={"error": "interrupted"}
            )
        )
    else:
        click.echo("Aborted.", err=True)
    raise click.exceptions.Exit(INTERRUPTED) from cause


def render_diagnostics(diagnostics: DiagnosticBag) -> None:
    """Print each diagnostic on stderr, as the domain renders it for people."""
    for diagnostic in diagnostics:
        click.echo(render_diagnostic(diagnostic), err=True)
