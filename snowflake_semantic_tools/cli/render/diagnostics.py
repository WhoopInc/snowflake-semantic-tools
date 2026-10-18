"""Findings and the verdict, as a person reads them on a terminal.

A finding is its severity and code, its message, the line it points at with a caret under the
column, then what to do: the suggestion, and the `sst explain` command for the code. The
verdict is the run's one closing line: whether it passed, what it counted, and how long it took,
then what was hidden and the next step. Colour and symbols come from `Style`, so the same
layout reads in ASCII and without colour.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from pathlib import Path

from snowflake_semantic_tools.cli.render.frames import HINT_KEYS, excerpt, pinpoint
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Diagnostic, Origin, Severity

_SEVERITY_ROLES = {Severity.ERROR: "error", Severity.WARNING: "warning", Severity.INFO: "info"}
# The flag that shows each kind of finding the human render counts instead of showing.
HIDDEN_FLAGS = {"info": "--show-info", "cascade": "--show-cascade", "baselined": "--show-baselined"}
_RELATED_SHOWN = 3


@dataclasses.dataclass(frozen=True)
class FindingNotes:
    """What the render says of one finding beyond the finding itself.

    Attributes:
        baselined: The run's baseline holds it, so it blocks nothing.
        hidden_cascades: Follow-on findings of this one the render counts instead of showing.
        fingerprint: The stable fingerprint `--verbose` prints; None prints no detail line.
    """

    baselined: bool = False
    hidden_cascades: int = 0
    fingerprint: str | None = None


@dataclasses.dataclass(frozen=True)
class Verdict:
    """The closing line of a run, and what follows it.

    Attributes:
        outcome: `ok`, `changes` (a difference found, as exit 2 reports), or `fail`.
        headline: What the run did, such as `validated 29 artifacts`, or `validate failed`.
        errors, warnings, baselined: Counted as `summary_counts` counts them; None leaves
            errors and warnings out, for a command that reports no diagnostics.
        hidden: How many findings of each kind in `HIDDEN_FLAGS` were counted, not shown.
        seconds: The run's wall time.
        next_step: The one next step to suggest; None suggests none.
        note: A disclosure the line ends with, such as why the output format was chosen.
    """

    outcome: str
    headline: str
    errors: int | None = None
    warnings: int = 0
    baselined: int = 0
    hidden: Mapping[str, int] = dataclasses.field(default_factory=dict)
    seconds: float | None = None
    next_step: str | None = None
    note: str | None = None


def render_finding(diagnostic: Diagnostic, style: Style, project_dir: Path, notes: FindingNotes) -> str:
    """Render one finding as lines of text, located, framed and followed by what to do.

    Example:
        error[SST-REF002]: 'customer_uuid' is not a column on customers
          --> metrics/customer_count.yml:3:3
           |
         3 |   expr: COUNT({{ ref('customers', 'customer_uuid') }})
           |   ^^^^
           = help: correct the column name, or add it to the model's columns: block
           = sst explain SST-REF002

    A finding the baseline holds is dim throughout, as it blocks nothing.
    """
    if notes.baselined:
        style = style.muted()
    origin = diagnostic.origin
    if origin is not None:
        hints = [str(diagnostic.context[key]) for key in HINT_KEYS if diagnostic.context.get(key) is not None]
        origin = pinpoint(project_dir, origin, hints)
    found = excerpt(project_dir, origin, style.terminal.width - 8) if origin is not None else None
    gutter = len(str(found.line)) if found is not None else 1
    pad = " " * (gutter + 1)
    role = _SEVERITY_ROLES[diagnostic.severity]
    lines = [_headline(diagnostic, style, role, notes)]
    if origin is not None:
        lines.append(f"{' ' * gutter}{style('-->', 'gutter')} {style(location(origin), 'path')}")
    if found is not None:
        bar = style("|", "gutter")
        carets = style("^" * found.length, role)
        lines += [f"{pad}{bar}", f"{style(str(found.line), 'gutter')} {bar} {found.text}"]
        lines.append(f"{pad}{bar} {' ' * found.start}{carets}")
    lines += [f"{pad}{item}" for item in _notes(diagnostic, style, notes)]
    return "\n".join(lines)


def render_collapsed(code: str, more: int, style: Style) -> str:
    """Render the line that stands for the further findings of one code a long run collapsed."""
    return style(f"  ... and {more} more {code} (--show-all-occurrences)", "dim")


def render_verdict(verdict: Verdict, style: Style) -> str:
    """Render the verdict: its line, then what was hidden, then the next step.

    Example:
        ✗ validate failed: 2 errors, 1 warning · 8 baselined · 1.1s
          hidden: 166 info (--show-info), 1 cascade (--show-cascade)
          next: fix the errors above, then run `sst validate`
    """
    symbol, role = {"ok": ("ok", "success"), "changes": ("changes", "changes")}.get(verdict.outcome, ("fail", "error"))
    parts = [_counts(verdict)] if verdict.errors is not None else []
    if verdict.baselined:
        parts.append(f"{verdict.baselined} baselined")
    if verdict.seconds is not None:
        parts.append(_seconds(verdict.seconds))
    joiner = ": " if verdict.outcome == "fail" and verdict.errors is not None else style.sep
    first = f"{style(style.symbol(symbol), role)} {style(verdict.headline, 'strong')}"
    tail = style.sep.join(parts)
    line = first + (joiner + tail if tail else "")
    if verdict.note:
        line += style(f" ({verdict.note})", "dim")
    lines = [line]
    hidden = [f"{count} {kind} ({HIDDEN_FLAGS[kind]})" for kind, count in verdict.hidden.items() if count]
    if hidden:
        lines.append(style(f"  hidden: {', '.join(hidden)}", "dim"))
    if verdict.next_step:
        lines.append(f"  {style('next:', 'help')} {verdict.next_step}")
    return "\n".join(lines)


def location(origin: Origin) -> str:
    """Return `file:line:col`, cut to what the origin knows."""
    parts = [origin.file]
    if origin.line is not None:
        parts.append(str(origin.line))
        if origin.col is not None:
            parts.append(str(origin.col))
    return ":".join(parts)


def plural(count: int, noun: str) -> str:
    """Return `count` and `noun`, plural unless the count is 1: `1 error`, `2 entries`, `3 info`."""
    if count == 1 or noun == "info":
        return f"{count} {noun}"
    if noun.endswith("y") and noun[-2:-1] not in ("a", "e", "o", "u"):
        return f"{count} {noun[:-1]}ies"
    return f"{count} {noun}s"


def located_message(diagnostic: Diagnostic) -> str:
    """Return the message without a `file:line:` prefix its template repeats from its origin."""
    message = diagnostic.message
    origin = diagnostic.origin
    if origin is None:
        return message
    parts = location(origin).split(":")
    for count in range(len(parts), 0, -1):
        prefix = ":".join(parts[:count]) + ": "
        if message.startswith(prefix):
            return message.removeprefix(prefix)
    return message


def _headline(diagnostic: Diagnostic, style: Style, role: str, notes: FindingNotes) -> str:
    severity = diagnostic.severity.name.lower()
    code = style.link(diagnostic.code, diagnostic.help_url)
    label = style(f"{severity}[", role) + style(code, role) + style("]", role)
    disclosed = _disclosed(diagnostic, notes)
    if disclosed:
        label += style(f" ({', '.join(disclosed)})", "dim")
    message_lines = located_message(diagnostic).splitlines() or [""]
    rest = "".join(f"\n    {line}" for line in message_lines[1:])
    return f"{label}: {style(message_lines[0], 'strong')}{rest}"


def _disclosed(diagnostic: Diagnostic, notes: FindingNotes) -> list[str]:
    """Say that the baseline holds a finding, then why it has the severity it shows."""
    found = ["baselined"] if notes.baselined else []
    if diagnostic.promoted_from is not None:
        found.append(f"promoted from {diagnostic.promoted_from.name.lower()}")
    elif diagnostic.demoted_from is not None and not diagnostic.cascaded:
        found.append(f"demoted from {diagnostic.demoted_from.name.lower()}")
    return found


def _notes(diagnostic: Diagnostic, style: Style, notes: FindingNotes) -> list[str]:
    """Return the `= ...` lines under a finding: help, related places, explain, detail, cascades."""
    equals = style("=", "gutter")
    lines: list[str] = []
    if ERROR_REGISTRY[diagnostic.code].suggestion and diagnostic.suggestion:
        lines.append(f"{equals} {style('help:', 'help')} {diagnostic.suggestion}")
    if diagnostic.related:
        places = ", ".join(location(item) for item in diagnostic.related[:_RELATED_SHOWN])
        more = len(diagnostic.related) - _RELATED_SHOWN
        lines.append(f"{equals} also at: {places}" + (f" and {more} more" if more > 0 else ""))
    lines.append(f"{equals} {style(f'sst explain {diagnostic.code}', 'dim')}")
    if notes.fingerprint is not None:
        lines.append(
            f"{equals} {style(f'phase: {diagnostic.phase}{style.sep}fingerprint: {notes.fingerprint}', 'dim')}"
        )
    if notes.hidden_cascades:
        noun = "finding" if notes.hidden_cascades == 1 else "findings"
        lines.append(f"{equals} {style(f'{notes.hidden_cascades} follow-on {noun} hidden (--show-cascade)', 'dim')}")
    return lines


def _counts(verdict: Verdict) -> str:
    return f"{plural(verdict.errors or 0, 'error')}, {plural(verdict.warnings, 'warning')}"


def _seconds(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rest = divmod(int(round(seconds)), 60)
    return f"{minutes}m{rest:02d}s"


def sorted_for_reading(diagnostics: Sequence[Diagnostic]) -> list[Diagnostic]:
    """Order findings errors first, then warnings, then info, keeping each group's report order.

    Report order is the pipeline's, so an artifact's first error still comes first within errors.
    """
    rank = {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}
    return sorted(diagnostics, key=lambda item: rank[item.severity])
