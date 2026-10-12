"""What `sst` prints: the one JSON envelope per run, diagnostics as JSON, and diagnostics as text.

`SstGroup.main` calls `start_invocation` before parsing, and the runner records what the run
resolved -- project, configuration file, target, overrides, baseline -- with `resolve_invocation`,
so every envelope reports the same context. The envelope lists every diagnostic, baselined or
not. The human render is what the global visibility flags shape: info, baselined, and
cascade-suppressed diagnostics are counted rather than shown unless asked for, four or more of one
code collapse to the first three, and none of it changes an exit code.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import NoReturn

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.app.evals.run import EvalRunsInterrupted
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, INTERRUPTED, OK
from snowflake_semantic_tools.cli.signals import received_signal
from snowflake_semantic_tools.domain.diagnostics import (
    ERROR_REGISTRY,
    D,
    Diagnostic,
    DiagnosticBag,
    render_diagnostic,
)
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

_INVOCATION: dict[str, object] = {}
# Credential values resolved this run; no envelope, report, or rendered diagnostic may carry one.
_SECRETS: set[str] = set()
_ARTIFACT_SUBJECTS = frozenset(SEMANTIC_REGISTRY.artifacts) | {"profile"}
# Four or more diagnostics of one code collapse to the first few in the human render.
_GROUP_AT = 4
_GROUP_SHOWN = 3


def start_invocation(argv: list[str]) -> None:
    """Record the command line and the time this run started, for every envelope it prints.

    The run's timings start from nothing, and switched off until the runner sees the run prints
    an envelope, so one process running `sst` twice reports each run alone.
    """
    _INVOCATION.clear()
    RUN_TIMINGS.reset()
    _INVOCATION.update(
        {
            "argv": argv,
            "started_at": SystemClock().now_iso(),
            "started_monotonic": time.monotonic(),
        }
    )


def resolve_invocation(
    *,
    project_dir: Path,
    config_file: Path | None,
    target: str | None,
    overrides: tuple[str, ...] = (),
    baselined: frozenset[str] = frozenset(),
) -> None:
    """Record what this run resolved, which every envelope it prints reports.

    Args:
        baselined: The stable fingerprints of the diagnostics the baseline holds.
    """
    _INVOCATION.update(
        {
            "project_dir": str(project_dir.resolve()),
            "config_file": str(config_file.resolve()) if config_file is not None else None,
            "target": target,
            "overrides": list(overrides),
            "baselined": baselined,
        }
    )


def register_secrets(values: Iterable[str]) -> None:
    """Record credential values this run resolved, which nothing it prints may contain."""
    _SECRETS.update(value for value in values if value)


def _leaks(text: str) -> bool:
    """Whether `text` carries a registered secret as written or as JSON escapes it."""
    return any(
        form in text
        for secret in _SECRETS
        for form in (secret, json.dumps(secret)[1:-1], json.dumps(secret, ensure_ascii=False)[1:-1])
    )


def _secret_refusal(where: str) -> Diagnostic:
    """Return the diagnostic printed in place of output that would carry a credential.

    Diagnostics:
        SST-PRT012: a resolved credential reached the output unredacted.
    """
    return D("SST-PRT012", subject="cli", value=f"a credential in the {where}")


def baselined_fingerprints() -> frozenset[str]:
    """Return the stable fingerprints of the diagnostics this run's baseline holds."""
    value = _INVOCATION.get("baselined")
    return value if isinstance(value, frozenset) else frozenset()


def record_reported(diagnostics: DiagnosticBag) -> None:
    """Record the diagnostics this run reports, once its policy, baseline, and audit are applied."""
    _INVOCATION["reported"] = diagnostics


def reported_diagnostics(default: DiagnosticBag) -> DiagnosticBag:
    """Return the diagnostics this run reports, as `record_reported` holds them; `default` before."""
    value = _INVOCATION.get("reported")
    return value if isinstance(value, DiagnosticBag) else default


def suppressed_by_baseline(diagnostics: Iterable[Diagnostic]) -> int:
    """Count the diagnostics this run's baseline holds, so suppresses; call once the baseline is matched."""
    baselined = baselined_fingerprints()
    return sum(stable_fingerprint(item) in baselined for item in diagnostics)


def summary_counts(diagnostics: Iterable[Diagnostic], baselined: frozenset[str] | None = None) -> dict[str, int]:
    """Count the diagnostics by severity, and those the baseline holds apart from every severity.

    A baselined diagnostic is counted once, under `baselined`, so a run whose every error the
    baseline holds -- a promoted warning under `--strict` -- reports no errors, as its exit says.

    Args:
        baselined: The fingerprints the baseline holds; this run's when None.
    """
    held = baselined_fingerprints() if baselined is None else baselined
    counts: Counter[str] = Counter()
    for item in diagnostics:
        counts["baselined" if stable_fingerprint(item) in held else item.severity.name.lower()] += 1
    return {key: counts[key] for key in ("error", "warning", "info", "baselined")}


def counted(count: int, noun: str) -> str:
    """Return `count` and `noun`, plural unless the count is 1: `1 error`, `2 errors`, `3 info`."""
    return f"{count} {noun}" if count == 1 or noun == "info" else f"{count} {noun}s"


def diagnostic_json(value: Diagnostic, *, baselined: bool = False) -> dict[str, object]:
    """Return one diagnostic as the envelope lists it, with its registered severity and suggestion.

    A subject that names an artifact type, or `profile`, is reported as `artifact`; any
    other `<type>:<name>` subject is a `member` of one. A code that declares `internal_detail`
    reports its raw `detail` value, the SST or Snowflake text beyond the template, as
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
) -> dict[str, object]:
    """Build the one JSON document `--output json` prints for a run of `command`.

    Without `exit_code`, the run exits 1 when a diagnostic is an error and 0 otherwise;
    without `status`, it follows the exit code: `ok`, `changes` for 2, else `error`. A callable
    `data` is called here, once the run's baseline is matched, for a payload that counts what
    the baseline suppressed. The summary counts as `summary_counts` does: a baselined diagnostic
    under `baselined`, and under no severity.
    """
    if callable(data):
        data = data()
    baselined = baselined_fingerprints()
    marked = [(diagnostic, stable_fingerprint(diagnostic) in baselined) for diagnostic in diagnostics]
    counts = summary_counts(diagnostics, baselined)
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
    }


def _invocation(command: str) -> dict[str, object]:
    """Return the envelope's `invocation`: the command line, target, project, config file, and timing.

    `timings` is `RUN_TIMINGS.snapshot()` with `wall_s`, the run's duration, beside it.
    """
    started_monotonic = _INVOCATION.get("started_monotonic")
    raw_argv = _INVOCATION.get("argv")
    duration = max(time.monotonic() - started_monotonic, 0.0) if isinstance(started_monotonic, float) else 0.0
    project_dir = _INVOCATION.get("project_dir")
    if project_dir is None:
        project_dir = str(Path(_invocation_option("--project-dir") or ".").resolve())
    overrides = _INVOCATION.get("overrides")
    return {
        "argv": [str(value) for value in raw_argv] if isinstance(raw_argv, list) else ["sst", command],
        "target": _INVOCATION.get("target", _invocation_option("--target")),
        "project_dir": project_dir,
        "config_file": _INVOCATION.get("config_file"),
        "overrides": overrides if isinstance(overrides, list) else [],
        "started_at": _INVOCATION.get("started_at"),
        "duration_s": round(duration, 6),
        "timings": {"wall_s": round(duration, 6), **RUN_TIMINGS.snapshot()},
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


def print_envelope(envelope: dict[str, object]) -> bool:
    """Print `envelope` on stdout as compact JSON with sorted keys, the one form `sst` emits.

    An envelope that would carry a credential is replaced by one reporting SST-PRT012 instead.

    Returns:
        True when the envelope was replaced, so the run must exit 1.
    """
    text = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    if _leaks(text):
        refusal = json_envelope(
            str(envelope.get("command", "")), DiagnosticBag((_secret_refusal("JSON envelope"),)), exit_code=ERROR
        )
        click.echo(json.dumps(refusal, sort_keys=True, separators=(",", ":")))
        return True
    click.echo(text)
    return False


def emit_json(envelope: dict[str, object], exit_code: int) -> NoReturn:
    """Print `envelope`, and exit with `exit_code`; with 1 when it was refused for carrying a credential."""
    refused = print_envelope(envelope)
    raise click.exceptions.Exit(ERROR if refused else exit_code)


def print_report(text: str, where: str) -> bool:
    """Print a YAML, CSV, or human report on stdout as written, unless it would carry a credential.

    A report that would is withheld, and SST-PRT012 is rendered on stderr in its place.

    Args:
        where: What the report is, as SST-PRT012 names it, such as `human report`.

    Returns:
        True when the report was withheld, so the run must exit 1.
    """
    if _leaks(text):
        render_diagnostics(DiagnosticBag((_secret_refusal(where),)))
        return True
    click.echo(text, nl=False)
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
    else:
        _echo_err(aborted_line())
        _echo_err(render_diagnostic(notice))
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
    _echo_err(aborted_line())
    if _hidden_reason(notice, frozenset(), _POLICY[0]) is not None:
        _echo_err(render_diagnostic(notice))


def _echo_err(text: str) -> None:
    with contextlib.suppress(OSError):
        click.echo(text, err=True)


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


class RenderPolicy:
    """How the human render shows diagnostics, from the global visibility and format flags.

    Attributes:
        color: Whether severity labels are coloured: `table` output on a terminal, unless
            `--no-color`, `$SST_NO_COLOR`, or a non-empty `$NO_COLOR` says otherwise.
        progress: Whether a long-running command prints progress lines on stderr: never with
            `--quiet`, nor with `--output json` or `yaml`, whose readers expect no other text.
    """

    def __init__(
        self,
        *,
        output: str = "table",
        no_color: bool = False,
        verbose: bool = False,
        quiet: bool = False,
        show_info: bool = False,
        show_baselined: bool = False,
        show_cascade: bool = False,
        show_all_occurrences: bool = False,
    ) -> None:
        self.color = output == "table" and not no_color and not os.environ.get("NO_COLOR") and sys.stderr.isatty()
        self.verbose = verbose
        self.quiet = quiet
        self.show_info = show_info
        self.show_baselined = show_baselined
        self.show_cascade = show_cascade
        self.show_all_occurrences = show_all_occurrences
        self.progress = output not in ("json", "yaml") and not quiet


_DEFAULT_POLICY = RenderPolicy()
_POLICY: list[RenderPolicy] = [_DEFAULT_POLICY]


def use_render_policy(policy: RenderPolicy) -> None:
    """Make `policy` the one every human render of this run follows."""
    _POLICY[0] = policy


def verbose_run() -> bool:
    """Report whether this run was asked for detailed progress, `--verbose`."""
    return _POLICY[0].verbose


def print_progress(line: str, *, warning: bool = False) -> None:
    """Print one progress line on stderr, as it happens, when this run's render policy shows progress.

    A warning is prefixed `warn`, as other command warnings are. A line that would carry a
    credential this run resolved is replaced by SST-PRT012.
    """
    policy = _POLICY[0]
    if not policy.progress:
        return
    text = f"warn  {line}" if warning else line
    click.echo(_render(_secret_refusal("progress output"), policy) if _leaks(text) else text, err=True)


def render_diagnostics(diagnostics: Iterable[Diagnostic]) -> None:
    """Print the diagnostics on stderr as this run's render policy shows them, then their counts.

    Baselined, info, and cascade diagnostics are counted rather than shown unless the matching
    `--show-*` flag asks; `--quiet` shows errors only. Four or more of one code show the first
    three and a count, unless `--show-all-occurrences`.
    """
    policy = _POLICY[0]
    bag = DiagnosticBag(diagnostics)
    if not bag:
        return
    baselined = baselined_fingerprints()
    hidden: Counter[str] = Counter()
    shown: list[Diagnostic] = []
    for diagnostic in bag:
        reason = _hidden_reason(diagnostic, baselined, policy)
        if reason is None:
            shown.append(diagnostic)
        else:
            hidden[reason] += 1
    seen: Counter[str] = Counter()
    totals = Counter(item.code for item in shown)
    for diagnostic in shown:
        seen[diagnostic.code] += 1
        grouped = totals[diagnostic.code] >= _GROUP_AT and not policy.show_all_occurrences
        if grouped and seen[diagnostic.code] > _GROUP_SHOWN:
            if seen[diagnostic.code] == totals[diagnostic.code]:
                more = totals[diagnostic.code] - _GROUP_SHOWN
                click.echo(f"  ... and {more} more {diagnostic.code} (--show-all-occurrences)", err=True)
            continue
        rendered = _render(diagnostic, policy)
        click.echo(_render(_secret_refusal("diagnostic text"), policy) if _leaks(rendered) else rendered, err=True)
    if not policy.quiet:
        click.echo(_summary(bag, baselined, hidden), err=True)


def _hidden_reason(diagnostic: Diagnostic, baselined: frozenset[str], policy: RenderPolicy) -> str | None:
    """Return why the human render counts `diagnostic` instead of showing it; None to show it."""
    if policy.quiet and not diagnostic.blocks:
        return "quiet"
    if stable_fingerprint(diagnostic) in baselined and not policy.show_baselined:
        return "baselined"
    # A cascade is always info, so `--show-cascade` shows it without `--show-info`.
    if diagnostic.cascaded:
        return None if policy.show_cascade else "cascade"
    if diagnostic.informational and not policy.show_info:
        return "info"
    return None


def _render(diagnostic: Diagnostic, policy: RenderPolicy) -> str:
    """Render one diagnostic as text, disclosing a promotion, coloured and detailed as the policy says."""
    text = render_diagnostic(diagnostic)
    label = f"{diagnostic.severity.name.lower()}[{diagnostic.code}]"
    declared = diagnostic.promoted_from
    replacement = label
    if policy.color:
        colour = {"ERROR": "red", "WARNING": "yellow", "INFO": "blue"}[diagnostic.severity.name]
        replacement = click.style(label, fg=colour, bold=True)
    if declared is not None:
        replacement += f" (promoted from {declared.name.lower()})"
    elif diagnostic.demoted_from is not None and not diagnostic.cascaded:
        replacement += f" (demoted from {diagnostic.demoted_from.name.lower()})"
    text = text.replace(label, replacement, 1)
    if policy.verbose:
        text += f"\n  phase: {diagnostic.phase}; fingerprint: {stable_fingerprint(diagnostic)}"
    return text


def _summary(diagnostics: DiagnosticBag, baselined: frozenset[str], hidden: Counter[str]) -> str:
    """Return the counts line: every severity, how many were baselined apart, and what was not shown."""
    counts = summary_counts(diagnostics, baselined)
    held = counts["baselined"]
    parts = [
        counted(counts["error"], "error"),
        counted(counts["warning"], "warning") + (f" ({held} baselined)" if held else ""),
        counted(counts["info"], "info"),
    ]
    flags = {"info": "--show-info", "baselined": "--show-baselined", "cascade": "--show-cascade"}
    notes = [f"{count} {reason} not shown ({flags[reason]})" for reason, count in hidden.items() if reason in flags]
    return "; ".join(parts) + (f"; {'; '.join(notes)}" if notes else "")
