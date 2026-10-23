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
import json
import time
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS
from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, INTERRUPTED, OK
from snowflake_semantic_tools.cli.render.diagnostics import (
    HIDDEN_FLAGS,
    FindingNotes,
    Verdict,
    render_collapsed,
    render_finding,
    render_verdict,
    sorted_for_reading,
)
from snowflake_semantic_tools.cli.render.next_steps import default_next_steps
from snowflake_semantic_tools.cli.render.progress import active_display
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import PLAIN, Terminal, system_terminal
from snowflake_semantic_tools.domain.diagnostics import (
    D,
    Diagnostic,
    DiagnosticBag,
)
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

_INVOCATION: dict[str, object] = {}
# Credential values resolved this run; no envelope, report, or rendered diagnostic may carry one.
_SECRETS: set[str] = set()
_ARTIFACT_SUBJECTS = frozenset(SEMANTIC_REGISTRY.artifacts) | {"coco_desktop_profile"}
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


def record_baselined(baselined: frozenset[str]) -> None:
    """Record which diagnostics the baseline holds, leaving the rest of the invocation as resolved.

    Args:
        baselined: The stable fingerprints of the diagnostics the baseline holds.
    """
    _INVOCATION["baselined"] = baselined


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


def scrubbed(text: str, where: str) -> str:
    """Return `text`, or SST-PRT012 rendered in its place when it would carry a resolved credential.

    Args:
        where: What the text is, as SST-PRT012 names it, such as `log output`.
    """
    return _render(_secret_refusal(where), _POLICY[0]) if _leaks(text) else text


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


def _echo_err(text: str) -> None:
    # click strips ANSI from a stream that is not a terminal; `--color always` keeps it.
    with contextlib.suppress(OSError):
        click.echo(text, err=True, color=True if _POLICY[0].terminal.color else None)


class RenderPolicy:
    """How the human render shows diagnostics, from the global visibility and format flags.

    Attributes:
        color: Whether stderr is coloured: `table` output on a terminal, as `--color` decides,
            unless `--no-color`, `$SST_NO_COLOR`, or a non-empty `$NO_COLOR` says otherwise.
        terminal: What the terminal can show; `detect_terminal` decides it once per run.
        progress: Whether a long-running command reports progress on stderr: never with
            `--quiet`, nor with `--output json`, `yaml` or `agent`, whose readers expect no
            other text.
        agent: The run prints the `agent` document instead of human text.
        output: The `--output` format the run prints, such as `table` or `plain`.
    """

    def __init__(
        self,
        *,
        output: str = "table",
        no_color: bool = False,
        color: str = "auto",
        verbose: bool = False,
        quiet: bool = False,
        show_info: bool = False,
        show_baselined: bool = False,
        show_cascade: bool = False,
        show_all_occurrences: bool = False,
        terminal: Terminal | None = None,
    ) -> None:
        choice = "never" if no_color else color
        self.terminal = terminal or system_terminal(choice=choice, colorable=output == "table")
        self.color = self.terminal.color
        self.verbose = verbose
        self.quiet = quiet
        self.show_info = show_info
        self.show_baselined = show_baselined
        self.show_cascade = show_cascade
        self.show_all_occurrences = show_all_occurrences
        self.agent = output == "agent"
        self.output = output
        self.progress = output in ("table", "plain", "csv") and not quiet


_DEFAULT_POLICY = RenderPolicy(terminal=PLAIN)
_POLICY: list[RenderPolicy] = [_DEFAULT_POLICY]


def use_render_policy(policy: RenderPolicy) -> None:
    """Make `policy` the one every human render of this run follows."""
    _POLICY[0] = policy


def render_policy() -> RenderPolicy:
    """Return the render policy this run follows."""
    return _POLICY[0]


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
    text = scrubbed(f"warn  {line}" if warning else line, "progress output")
    display = active_display()
    if display is not None:
        display.line(text)
    else:
        click.echo(text, err=True)


def announce(*parts: str) -> None:
    """Print a fact about the run, such as the account it connected to, where progress shows.

    The parts are joined by the separator the terminal can show. The line is printed only while
    a progress display shows, on a terminal or in CI, so a short run and every test see nothing.
    """
    display = active_display()
    if display is not None and _POLICY[0].progress:
        style = Style(_POLICY[0].terminal)
        display.line(f"{style(style.symbol('ok'), 'success')} {style.sep.join(parts)}")


def render_diagnostics(diagnostics: Iterable[Diagnostic]) -> None:
    """Print the diagnostics on stderr as this run's render policy shows them, most severe first.

    Baselined, info, and cascade diagnostics are counted rather than shown unless the matching
    `--show-*` flag asks, and `print_verdict` reports the counts; `--quiet` shows errors only.
    A hidden cascade is counted under the finding it follows from. Four or more of one code
    show the first three and a count, unless `--show-all-occurrences`.
    """
    policy = _POLICY[0]
    shown, _ = _partition(DiagnosticBag(diagnostics), policy)
    cascades = _cascades_by_cause(DiagnosticBag(diagnostics), shown, policy)
    style = Style(policy.terminal)
    seen: Counter[str] = Counter()
    totals = Counter(item.code for item in shown)
    for index, diagnostic in enumerate(shown):
        seen[diagnostic.code] += 1
        grouped = totals[diagnostic.code] >= _GROUP_AT and not policy.show_all_occurrences
        if grouped and seen[diagnostic.code] > _GROUP_SHOWN:
            if seen[diagnostic.code] == totals[diagnostic.code]:
                _echo_err(render_collapsed(diagnostic.code, totals[diagnostic.code] - _GROUP_SHOWN, style))
                if not policy.quiet:
                    _echo_err("")
            continue
        _echo_err(scrubbed(_render(diagnostic, policy, hidden_cascades=cascades.get(index, 0)), "diagnostic text"))
        if not policy.quiet:
            _echo_err("")


def hidden_counts(diagnostics: DiagnosticBag) -> dict[str, int]:
    """Count the diagnostics the human render hides, by why: info, cascade, or baselined."""
    _, hidden = _partition(diagnostics, _POLICY[0])
    return {kind: hidden[kind] for kind in HIDDEN_FLAGS if hidden[kind]}


def print_verdict(
    command: str,
    exit_code: int,
    diagnostics: DiagnosticBag,
    *,
    headline: str | None = None,
    failure: str | None = None,
    next_step: str | None = None,
    counts: bool = True,
    hidden: bool = True,
) -> None:
    """Print the run's closing line on stderr: how it ended, its counts, wall time, then the next step.

    A run that succeeded says what it did when `headline` does, and `<command> passed` otherwise,
    or `<command>: differences found` for exit 2; one that failed says `<command> failed`, and one
    interrupted `<command> interrupted`. Nothing
    is printed under `--quiet`. The line is checked for credentials as every report is.

    Args:
        failure: What a failure stopped on, said after `failed`: `plan failed (2 blocked)`.
        counts: False leaves the error and warning counts out, for a command that reports none;
            they are left out of a failure that counted neither, too.
        hidden: False leaves out what the render hid, for a command that shows no diagnostics.
    """
    policy = _POLICY[0]
    if policy.quiet or policy.agent:
        return
    summary = summary_counts(diagnostics)
    outcome = "ok" if exit_code == OK else "changes" if exit_code == CHANGES else "fail"
    if outcome == "fail":
        title = f"{command} interrupted" if exit_code == INTERRUPTED else f"{command} failed"
        title += f" ({failure})" if failure and exit_code != INTERRUPTED else ""
    else:
        title = headline or (f"{command} passed" if outcome == "ok" else f"{command}: differences found")
    verdict = Verdict(
        outcome=outcome,
        headline=title,
        # A failure nothing counted (a smoke run, an eval gate, a check) is not "0 errors": it reports its own way.
        errors=summary["error"] if _counted(counts, outcome, summary) else None,
        warnings=summary["warning"],
        baselined=summary["baselined"],
        hidden=hidden_counts(diagnostics) if hidden else {},
        seconds=run_seconds(),
        next_step=next_step,
        note=output_note(),
    )
    _echo_err(scrubbed(render_verdict(verdict, Style(policy.terminal)), "verdict"))


def _counted(counts: bool, outcome: str, summary: dict[str, int]) -> bool:
    """Report whether the verdict counts errors and warnings, as `print_verdict` decides."""
    found = bool(summary["error"] or summary["warning"])
    return found or (counts and outcome != "fail")


def next_steps_for(
    command: str,
    exit_code: int,
    diagnostics: DiagnosticBag,
    given: tuple[str, ...] | None,
    *,
    audience: str = "person",
) -> tuple[str, ...]:
    """Return the run's next steps: those the command gave, else the defaults for how it ended.

    A setup failure (exit 4) whose one error says how to fix it, as SST-MAN009's "run sst
    compile" does, takes that fix as its step instead of the generic one.
    """
    if given is not None:
        return given
    counts = summary_counts(diagnostics)
    errors = [item for item in diagnostics if item.blocks and stable_fingerprint(item) not in baselined_fingerprints()]
    if exit_code == CONFIG and len(errors) == 1 and errors[0].suggestion:
        return (errors[0].suggestion,)
    return default_next_steps(command, exit_code, errors=counts["error"], warnings=counts["warning"], audience=audience)


def first_next_step(command: str, exit_code: int, diagnostics: DiagnosticBag) -> str | None:
    """Return the first default next step for how the run ended; None when there is none."""
    steps = next_steps_for(command, exit_code, diagnostics, None)
    return steps[0] if steps else None


def report_failure(command: str, diagnostics: DiagnosticBag, exit_code: int) -> None:
    """Print a run that failed before its report: its diagnostics, then its verdict, on stderr."""
    render_diagnostics(diagnostics)
    print_verdict(command, exit_code, diagnostics, next_step=first_next_step(command, exit_code, diagnostics))


def run_seconds() -> float | None:
    """Return how long this run has taken so far; None before `start_invocation` records its start."""
    started = _INVOCATION.get("started_monotonic")
    return max(time.monotonic() - started, 0.0) if isinstance(started, float) else None


def note_output_choice(note: str | None) -> None:
    """Record why this run chose its output format, which its verdict and agent document disclose."""
    _INVOCATION["output_note"] = note


def output_note() -> str | None:
    """Return why this run chose its output format; None when it was asked for."""
    value = _INVOCATION.get("output_note")
    return value if isinstance(value, str) else None


def project_root() -> Path:
    """Return the project directory this run resolved, which finding locations are relative to."""
    value = _INVOCATION.get("project_dir")
    return Path(value) if isinstance(value, str) else Path(".")


def _partition(diagnostics: DiagnosticBag, policy: RenderPolicy) -> tuple[list[Diagnostic], Counter[str]]:
    """Return the diagnostics the render shows, most severe first, and the count it hides by reason."""
    baselined = baselined_fingerprints()
    hidden: Counter[str] = Counter()
    shown: list[Diagnostic] = []
    for diagnostic in diagnostics:
        reason = _hidden_reason(diagnostic, baselined, policy)
        if reason is None:
            shown.append(diagnostic)
        else:
            hidden[reason] += 1
    return sorted_for_reading(shown), hidden


def _cascades_by_cause(diagnostics: DiagnosticBag, shown: list[Diagnostic], policy: RenderPolicy) -> dict[int, int]:
    """Count each hidden cascade under the shown finding it follows from, by that finding's index.

    A cascade names its cause's code, not the finding: it goes under the first shown finding of
    that code about the same subject, else the first of that code; one with neither is counted
    in the verdict alone.
    """
    if policy.show_cascade:
        return {}
    counts: Counter[int] = Counter()
    for cascade in (item for item in diagnostics if item.cascaded):
        same_code = [index for index, item in enumerate(shown) if item.code == cascade.caused_by]
        same_subject = [index for index in same_code if shown[index].subject == cascade.subject]
        targets = same_subject or same_code
        if targets:
            counts[targets[0]] += 1
    return dict(counts)


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


def refusal_text(diagnostic: Diagnostic) -> str:
    """Render a refusal made before the run's options are read, such as a usage error, for stderr.

    Colour follows the environment alone (`$NO_COLOR`, a terminal), since `--no-color` may be
    the very flag that did not parse.
    """
    terminal = system_terminal(choice="auto", colorable=True)
    return render_finding(diagnostic, Style(terminal), project_root(), FindingNotes())


def _render(diagnostic: Diagnostic, policy: RenderPolicy, *, hidden_cascades: int = 0) -> str:
    """Render one diagnostic as a framed finding, disclosing a promotion and the baseline's hold."""
    fingerprint = stable_fingerprint(diagnostic)
    notes = FindingNotes(
        baselined=fingerprint in baselined_fingerprints(),
        hidden_cascades=hidden_cascades,
        fingerprint=fingerprint if policy.verbose else None,
    )
    return render_finding(diagnostic, Style(policy.terminal), project_root(), notes)
