"""What a run reports as data: the JSON and agent envelopes, one diagnostic as JSON, and an interrupt.

`output` keeps the run's state (invocation, secrets, render policy) and the human render; this
module reads that state through `output` at call time, so whatever sets it is seen here.
"""

from __future__ import annotations

import contextlib
import io
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app.evals.run import EvalRunsInterrupted
from snowflake_semantic_tools.cli import output as _output
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, INTERRUPTED, OK
from snowflake_semantic_tools.cli.render.agent import agent_document
from snowflake_semantic_tools.cli.signals import received_signal
from snowflake_semantic_tools.domain.diagnostics import (
    ERROR_REGISTRY,
    D,
    Diagnostic,
    DiagnosticBag,
)
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key


def diagnostic_json(value: Diagnostic, *, baselined: bool = False) -> dict[str, object]:
    """Return one diagnostic as the envelope lists it, with its registered severity and suggestion.

    A subject that names an artifact type, or `profile`, is reported as `artifact`; any
    other `<type>:<name>` subject is a `member` of one. A code that declares `internal_detail`
    reports its `detail` value, the same text its message shows, on its own as
    `internal_detail`; every other code reports None.
    """
    origin = getattr(value, "origin", None)
    spec = ERROR_REGISTRY[value.code]
    subject_parts = split_artifact_key(value.subject) if value.subject and ":" in value.subject else None
    return {
        "code": value.code,
        "severity": value.severity.name.lower(),
        "declared_severity": spec.severity.name.lower(),
        "promoted_from": value.promoted_from.name.lower() if value.promoted_from is not None else None,
        "demoted_from": value.demoted_from.name.lower() if value.demoted_from is not None else None,
        "baselined": baselined,
        "message": value.message,
        "suggestion": value.suggestion,
        "phase": value.phase,
        "help_url": value.help_url,
        "fingerprint": stable_fingerprint(value),
        "params": dict(value.context),
        "artifact": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] in _output._ARTIFACT_SUBJECTS
            else None
        ),
        "member": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] not in _output._ARTIFACT_SUBJECTS
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
        "internal_detail": value.context.get("detail") if spec.internal_detail else None,
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
    next_steps: tuple[str, ...] | None = None,
) -> dict[str, object]:
    """Build the one JSON document `--output json` prints for a run of `command`.

    Without `exit_code`, the run exits 1 when a diagnostic is an error and 0 otherwise;
    without `status`, it follows the exit code: `ok`, `changes` for 2, else `error`. A callable
    `data` is called here, once the run's baseline is matched, for a payload that counts what
    the baseline suppressed. The summary counts as `_output.summary_counts` does: a baselined diagnostic
    under `baselined`, and under no severity. `next_steps` is what to do next, first step first;
    None takes the defaults for the command and its exit code, as `_output.next_steps_for` gives them.
    """
    if callable(data):
        data = data()
    baselined = _output.baselined_fingerprints()
    marked = [(diagnostic, stable_fingerprint(diagnostic) in baselined) for diagnostic in diagnostics]
    counts = _output.summary_counts(diagnostics, baselined)
    resolved_exit = exit_code if exit_code is not None else ERROR if counts["error"] else OK
    return {
        "tool": "sst",
        "sst_version": VERSION,
        "schema_version": 2,
        "command": command,
        "status": status
        or ("error" if resolved_exit not in (OK, CHANGES) else "changes" if resolved_exit == CHANGES else "ok"),
        "exit_code": resolved_exit,
        "invocation": _invocation(command),
        "diagnostics": [diagnostic_json(diagnostic, baselined=flag) for diagnostic, flag in marked],
        "summary": {
            "error": counts["error"],
            "warning": counts["warning"],
            "info": counts["info"],
            "promoted": promoted,
            "suppressed_cascade": sum(diagnostic.cascaded for diagnostic in diagnostics),
            "baselined": counts["baselined"],
        },
        "data": data if data is not None else {},
        "next_steps": list(_output.next_steps_for(command, resolved_exit, diagnostics, next_steps, audience="agent")),
    }


def _invocation(command: str) -> dict[str, object]:
    """Return the envelope's `invocation`: the command line, target, project, config file, and timing.

    `timings` is `RUN_TIMINGS.snapshot()` with `wall_s`, the run's duration, beside it.
    """
    started_monotonic = _output._INVOCATION.get("started_monotonic")
    raw_argv = _output._INVOCATION.get("argv")
    duration = max(time.monotonic() - started_monotonic, 0.0) if isinstance(started_monotonic, float) else 0.0
    project_dir = _output._INVOCATION.get("project_dir")
    if project_dir is None:
        project_dir = str(Path(_invocation_option("--project-dir") or ".").resolve())
    overrides = _output._INVOCATION.get("overrides")
    return {
        "argv": [str(value) for value in raw_argv] if isinstance(raw_argv, list) else ["sst", command],
        "target": _output._INVOCATION.get("target", _invocation_option("--target")),
        "project_dir": project_dir,
        "config_file": _output._INVOCATION.get("config_file"),
        "overrides": overrides if isinstance(overrides, list) else [],
        "started_at": _output._INVOCATION.get("started_at"),
        "duration_s": round(duration, 6),
        "timings": {"wall_s": round(duration, 6), **RUN_TIMINGS.snapshot()},
    }


def _invocation_option(name: str) -> str | None:
    """Return the value given for option `name` on the recorded command line, as `--x v` or `--x=v`."""
    argv = _output._INVOCATION.get("argv", ())
    if not isinstance(argv, list):
        return None
    for index, value in enumerate(argv):
        if value == name and index + 1 < len(argv):
            return str(argv[index + 1])
        if isinstance(value, str) and value.startswith(f"{name}="):
            return value.split("=", 1)[1]
    return None


def print_envelope(envelope: dict[str, object]) -> bool:
    """Print `envelope` on stdout as compact JSON with sorted keys, the one form `sst` emits.

    An envelope that would carry a credential is replaced by one reporting SST-PRT012 instead.

    Returns:
        True when the envelope was replaced, so the run must exit 1.
    """
    text = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    if _output._leaks(text):
        refusal = json_envelope(
            str(envelope.get("command", "")),
            DiagnosticBag((_output._secret_refusal("JSON envelope"),)),
            exit_code=ERROR,
        )
        click.echo(json.dumps(refusal, sort_keys=True, separators=(",", ":")))
        return True
    click.echo(text)
    return False


def emit_json(envelope: dict[str, object], exit_code: int) -> NoReturn:
    """Print `envelope`, and exit with `exit_code`; with 1 when it was refused for carrying a credential."""
    refused = print_envelope(envelope)
    raise click.exceptions.Exit(ERROR if refused else exit_code)


def emit_agent(
    command: str,
    diagnostics: DiagnosticBag,
    exit_code: int,
    *,
    data: object = None,
    next_steps: tuple[str, ...] | None = None,
) -> NoReturn:
    """Print the run's `agent` document on stdout, and exit with `exit_code`.

    The document is checked for credentials as an envelope is: one that would carry a credential
    is withheld, SST-PRT012 printed in its place, and the run exits 1.
    """
    steps = _output.next_steps_for(command, exit_code, diagnostics, next_steps, audience="agent")
    document = agent_document(
        command,
        exit_code,
        diagnostics,
        baselined=_output.baselined_fingerprints(),
        fingerprint=stable_fingerprint,
        project_dir=_output.project_root(),
        data=data,
        seconds=_output.run_seconds(),
        note=_output.output_note(),
        next_steps=steps,
    )
    if _output._leaks(document):
        refusal = DiagnosticBag((_output._secret_refusal("agent document"),))
        click.echo(
            agent_document(
                command,
                ERROR,
                refusal,
                baselined=frozenset(),
                fingerprint=stable_fingerprint,
                project_dir=_output.project_root(),
            ),
            nl=False,
        )
        raise click.exceptions.Exit(ERROR)
    with contextlib.suppress(OSError):
        click.echo(document, nl=False)
    raise click.exceptions.Exit(exit_code)


def print_report(text: str, where: str) -> bool:
    """Print a YAML, CSV, or human report on stdout as written, unless it would carry a credential.

    A report that would is withheld, and SST-PRT012 is rendered on stderr in its place.

    Args:
        where: What the report is, as SST-PRT012 names it, such as `human report`.

    Returns:
        True when the report was withheld, so the run must exit 1.
    """
    if _output._leaks(text):
        _output.render_diagnostics(DiagnosticBag((_output._secret_refusal(where),)))
        return True
    click.echo(text, nl=False, color=True if _output._POLICY[0].terminal.color_out else None)
    return False


def captured(report: Callable[[], None]) -> str:
    """Return what `report` prints on stdout, printing none of it, so it can be checked first."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        report()
    return buffer.getvalue()


def interrupted(command: str, output: str, cause: BaseException) -> NoReturn:
    """End a run the user interrupted or declined to confirm with exit 130, never an internal error.

    An interrupt is Ctrl-C, or SIGTERM or SIGHUP as `cli.signals` raises them; the human report
    names the signal. Text that cannot be written, as after SIGHUP closed the terminal, is
    dropped rather than changing the exit code.

    Diagnostics:
        SST-PRT107: the run was interrupted; after `apply`, partial work may have been applied,
            and after an eval run started, it names every run, which goes on in Snowflake.
    """
    notice = D("SST-PRT107", subject="cli", detail=f"sst {command} started", value=_what_remains(command, cause))
    if output == "json":
        print_envelope(
            json_envelope(
                command, DiagnosticBag((notice,)), exit_code=INTERRUPTED, status="error", data={"error": "interrupted"}
            )
        )
    elif output == "agent":
        emit_agent(command, DiagnosticBag((notice,)), INTERRUPTED)
    else:
        _output._echo_err(aborted_line())
        _output._echo_err(_output._render(notice, _output._POLICY[0]))
    raise click.exceptions.Exit(INTERRUPTED) from cause


def aborted_line() -> str:
    """Return the line an interrupted run starts its human report with, naming the signal if one ended it."""
    name = received_signal()
    return f"Aborted: received {name}." if name else "Aborted."


def announce_interrupt(notice: Diagnostic) -> None:
    """Print that the run was interrupted, then `notice` when the render policy would only count it.

    For a command that reports an interrupted run as a result rather than through `interrupted`;
    the notice is in the result's diagnostics as well, for `--output json`.
    """
    _output._echo_err(aborted_line())
    if _output._hidden_reason(notice, frozenset(), _output._POLICY[0]) is not None:
        _output._echo_err(_output._render(notice, _output._POLICY[0]))


def _what_remains(command: str, cause: BaseException) -> str:
    """Say what an interrupted run leaves in Snowflake: started eval runs, partial work, or nothing."""
    if isinstance(cause, EvalRunsInterrupted):
        return (
            f"evaluation runs were started and may still be running: {', '.join(cause.run_names)}; "
            "check their status or cancel them in Snowflake"
        )
    if command == "apply":
        return "partial work may have been applied; run sst plan to see what remains"
    return "nothing was written to Snowflake"
