"""Reports a person reads on stdout: aligned tables, the plan, and what apply did.

Rows are laid out in columns fitted to the terminal, the widest column cut first. A plan lists
what it would change in the order apply would change it, and counts what stays the same, so the
few lines that matter are not lost among the unchanged ones; `--verbose` lists those too.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.cli.render.frames import display_width
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOutcome, Change, ChangeSet
from snowflake_semantic_tools.domain.model.lifecycle.action import ChangeReason
from snowflake_semantic_tools.domain.model.lifecycle.apply import OutcomeStatus
from snowflake_semantic_tools.domain.plan.member_diff import (
    ADDED,
    REMOVED,
    MemberDiff,
    printable,
    property_lines,
    summary,
)
from snowflake_semantic_tools.domain.plan.properties import changed_properties

_ACTION_STYLE = {
    Action.CREATE: ("+", "create", "create"),
    Action.UPDATE: ("~", "update", "update"),
    Action.PRUNE: ("-", "prune", "prune"),
    Action.BLOCKED: ("!", "blocked", "blocked"),
    Action.NOOP: ("=", "unchanged", "dim"),
}
# What each reason says to a person; one that says nothing beyond its action says nothing.
_REASONS = {
    ChangeReason.NOT_PRESENT: "",
    ChangeReason.FINGERPRINT_DIFFERS: "",
    ChangeReason.NO_PRIOR_STATE: "exists, no state entry yet",
    ChangeReason.STATE_MANIFEST_MISMATCH: "matches; state is re-recorded",
    ChangeReason.UNCHANGED: "",
    ChangeReason.ORPHANED: "no longer declared",
    ChangeReason.VALIDATION_ERRORS: "has errors",
    ChangeReason.DEPENDENCY_BLOCKED: "depends on a blocked change",
    ChangeReason.UNMANAGED_OBJECT: "exists and SST does not own it",
    ChangeReason.TARGET_MOVED: "state records another target",
    ChangeReason.UNRECORDED_PUBLISH: "published but not recorded",
    ChangeReason.VERSION_DRIFT: "its alias or live version drifted",
}
_STATUS_STYLE = {
    OutcomeStatus.APPLIED: ("ok", "success"),
    OutcomeStatus.FAILED: ("fail", "error"),
    OutcomeStatus.SKIPPED: ("changes", "dim"),
}
_MAX_LISTED_PROPERTIES = 4
# What each property an update changes means to a person. The marker records the manifest that
# wrote the object, so it changes with every definition and is named only when it changes alone.
_PROPERTY_LABELS = {
    "target": "renamed",
    "fingerprint": "definition",
    "manifest_id": "ownership marker",
    "aliases": "version alias",
    "tags": "tags",
}
# How much of an update's member diff a plan prints without `--full`: lines per property, lines
# per change, and characters per line. The diff file holds all of it.
DIFF_PROPERTY_LINES = 12
DIFF_CHANGE_LINES = 40
DIFF_LINE_CHARS = 160
# The role each member and line sign is painted in.
_MEMBER_MARKS = {ADDED: ("+", "create"), REMOVED: ("-", "prune")}
_LINE_ROLES = {"-": "before", "+": "after", "~": "update", " ": "dim"}


def table(rows: Sequence[Sequence[str]], width: int, *, gap: int = 2, keep: int | None = None) -> list[str]:
    """Lay rows out in left-aligned columns within `width`; cells are plain text, styled after.

    The last column takes what is left and may run past `width`; the earlier columns are cut,
    the widest first, with `...`, only while they leave the last column less than a third of
    the width (at least 20 columns), and none below 12. Column `keep`, the one that names
    each row, is cut only once no other can be. A cut qualified name keeps its first and last
    parts, the database and the object, else its object alone, so names in one schema differ.

    Example:
        `[["+", "semantic_view:sales", "SALES_DB.SV.SALES"]]` lays out as
        `+  semantic_view:sales  SALES_DB.SV.SALES`.
    """
    if not rows:
        return []
    count = max(len(row) for row in rows)
    widths = [max((display_width(row[index]) for row in rows if index < len(row)), default=0) for index in range(count)]
    room_for_last = min(widths[-1], max(20, width // 3))
    while sum(widths[:-1]) + room_for_last + gap * (count - 1) > width and count > 1:
        cuttable = [index for index in range(count - 1) if widths[index] > 12]
        others = [index for index in cuttable if index != keep]
        if not cuttable:
            break
        widths[max(others or cuttable, key=lambda index: widths[index])] -= 1
    lines = []
    for row in rows:
        cells = [_cut(cell, widths[index]) if index < count - 1 else cell for index, cell in enumerate(row)]
        padded = [cell + " " * (widths[index] - display_width(cell)) for index, cell in enumerate(cells[:-1])]
        lines.append((" " * gap).join([*padded, cells[-1]]).rstrip())
    return lines


def plan_lines(
    changeset: ChangeSet,
    style: Style,
    *,
    verbose: bool = False,
    full: bool = False,
    more: str = "--full shows all",
) -> list[str]:
    """Return the plan as a person reads it: each change in apply order, then what stays the same.

    An update names the members it changes, or the properties when it was not compared member by
    member, and is followed by what changes in each member, cut short where long with `more` said
    of the rest. A prune SST only reports is marked `report only`. Unchanged artifacts are
    counted on one line, unless `verbose` lists them. With `full`, the member diff is not cut, and
    each update is followed by its properties' before and after, and the definition it replaces
    when `--capture-prior` read it.

    Example:
        `  + create  semantic_view:sales_partner  SALES_DB.SEMANTIC_VIEWS.SALES_PARTNER`
        `  ~ update  semantic_view:sales  SALES_DB.SEMANTIC_VIEWS.SALES  metric orders.total_revenue`
        `      ~ metric orders.total_revenue`
        `          comment:`
        `            - Revenue before tax.`
        `            + Revenue before tax, in dollars.`
        `  29 unchanged (--verbose lists them)`
    """
    listed = [change for change in changeset.changes if verbose or change.action is not Action.NOOP]
    rows = [_plan_row(change, changeset.manifest_id) for change in listed]
    laid = table(rows, style.terminal.width - 2, keep=1)
    lines: list[str] = []
    for change, row, text in zip(listed, rows, laid, strict=True):
        head = row[0]
        lines.append("  " + style(head, _ACTION_STYLE[change.action][2]) + text[len(head) :])
        if change.action is Action.UPDATE and change.member_diff:
            lines += member_diff_lines(change.member_diff, style, full=full, more=more)
        if full:
            lines += _detail(change, changeset.manifest_id, style)
    unchanged = sum(change.action is Action.NOOP for change in changeset.changes)
    if unchanged and not verbose:
        lines.append("  " + style(f"{unchanged} unchanged (--verbose lists them)", "dim"))
    return lines


def plan_headline(changeset: ChangeSet) -> str:
    """Return what the plan would do, in one phrase, for its verdict.

    Example:
        `plan: 1 to create, 2 to update, 0 to prune; 26 unchanged`, or
        `plan: nothing to change; 29 unchanged`.
    """
    counts = {action: sum(change.action is action for change in changeset.changes) for action in Action}
    report_only = len(changeset.report_only)
    prunes = counts[Action.PRUNE] - report_only
    changing = counts[Action.CREATE] + counts[Action.UPDATE] + prunes
    parts = (
        [f"{counts[Action.CREATE]} to create", f"{counts[Action.UPDATE]} to update", f"{prunes} to prune"]
        if changing
        else ["nothing to change"]
    )
    tail = [f"{counts[Action.NOOP]} unchanged"]
    if counts[Action.BLOCKED]:
        tail.append(f"{counts[Action.BLOCKED]} blocked")
    if report_only:
        tail.append(f"{report_only} report-only")
    return f"plan: {', '.join(parts)}; {', '.join(tail)}"


def outcome_lines(outcomes: Sequence[ApplyOutcome], style: Style) -> list[str]:
    """Return what apply did, one line per change it made or tried: status, action, key, error.

    An unchanged artifact is left out; `apply_headline` counts them.

    Example:
        `  ✓ created  semantic_view:sales_partner  1.2s`
        `  ✗ failed   agent:jaffle_analyst         insufficient privileges`
    """
    outcomes = [item for item in outcomes if item.action is not Action.NOOP]
    rows = []
    for outcome in outcomes:
        verb = _past(outcome)
        detail = outcome.error.message.splitlines()[0] if outcome.error else f"{outcome.duration_ms / 1000:.1f}s"
        rows.append([f"{style.symbol(_STATUS_STYLE[outcome.status][0])} {verb}", outcome.key, detail])
    lines = []
    for outcome, text, row in zip(outcomes, table(rows, style.terminal.width - 2, keep=1), rows, strict=True):
        role = _STATUS_STYLE[outcome.status][1]
        head = row[0]
        lines.append("  " + style(head, role) + text[len(head) :])
    return lines


def apply_headline(outcomes: Sequence[ApplyOutcome]) -> str:
    """Return what apply did, in one phrase, for its verdict: `applied 3 changes; 26 unchanged`."""
    applied = [item for item in outcomes if item.status is OutcomeStatus.APPLIED and item.action is not Action.NOOP]
    unchanged = sum(item.action is Action.NOOP for item in outcomes)
    noun = "change" if len(applied) == 1 else "changes"
    return f"applied {len(applied)} {noun}" + (f"; {unchanged} unchanged" if unchanged else "")


def _plan_row(change: Change, manifest_id: str) -> list[str]:
    """Return one change's cells: its mark and action, key, target, and what a person should know of it.

    The note says the reason when the action alone does not, the properties an update changes,
    that a prune is only reported, and the alias an agent's version moves to.
    """
    marker, action, _ = _ACTION_STYLE[change.action]
    if change.rendered is not None:
        target = change.rendered.target.sql
    else:
        target = change.observed.qualified_name.sql if change.observed is not None else "-"
    notes = [_REASONS.get(change.reason, change.reason.value)]
    if change.action is Action.UPDATE:
        names = _changed_labels(change, manifest_id)
        if names:
            more = len(names) - _MAX_LISTED_PROPERTIES
            notes.append(", ".join(names[:_MAX_LISTED_PROPERTIES]) + (f" and {more} more" if more > 0 else ""))
    if change.action is Action.PRUNE and not change.prune_executable:
        notes.append("report only: SST never removes these")
    alias = dict(change.rendered.component_fingerprints).get("alias") if change.rendered is not None else None
    if alias:
        notes.append(f"alias {alias}")
    return [f"{marker} {action}", change.key, target, "; ".join(note for note in notes if note)]


def _changed_labels(change: Change, manifest_id: str) -> list[str]:
    """Return what an update changes, as a person names it: its members, `version alias`, ...

    An update compared member by member names each member it changes in place of `definition`,
    or says no member differs when only what the comparison cannot see changed.
    """
    labels = property_labels([item.name for item in changed_properties(change, manifest_id)])
    if change.member_diff is None or "definition" not in labels:
        return labels
    members = list(summary(change.member_diff)) or ["definition (no member-level difference)"]
    at = labels.index("definition")
    return [*labels[:at], *members, *labels[at + 1 :]]


def property_labels(names: Sequence[str]) -> list[str]:
    """Return the properties an update changes as a person names them, the marker only when alone.

    Example:
        `["fingerprint", "manifest_id", "aliases"]` is `["definition", "version alias"]`.
    """
    if "fingerprint" in names:
        names = [name for name in names if name != "manifest_id"]
    return [_PROPERTY_LABELS.get(name, name) for name in names]


def member_diff_lines(diffs: Sequence[MemberDiff], style: Style, *, full: bool, more: str) -> list[str]:
    """Return an update's member diff as a person reads it, under the update's row.

    Each member is headed `~`, `+` or `-` for a changed, added or removed one; a changed member
    lists each property it changes, then the property's lines. An added or removed member lists
    its properties only with `full`. Without it, a property shows at most `DIFF_PROPERTY_LINES`
    lines, the whole diff `DIFF_CHANGE_LINES`, and a line `DIFF_LINE_CHARS` characters, the rest
    counted on a last line that says `more`.
    """
    lines: list[str] = []
    hidden = 0
    for item in diffs:
        mark, role = _MEMBER_MARKS.get(item.op, ("~", "update"))
        lines.append("      " + style(f"{mark} {printable(item.label)}", role))
        if item.op in (ADDED, REMOVED) and not full:
            continue
        for prop in item.properties:
            body = property_lines(prop)
            shown = body if full else body[:DIFF_PROPERTY_LINES]
            hidden += len(body) - len(shown)
            lines.append(f"          {printable(prop.name)}:")
            lines += ["            " + style(f"{sign} {_clip(text, full)}", _LINE_ROLES[sign]) for sign, text in shown]
    if not full and len(lines) > DIFF_CHANGE_LINES:
        hidden += len(lines) - DIFF_CHANGE_LINES
        lines = lines[:DIFF_CHANGE_LINES]
    if hidden:
        noun = "line" if hidden == 1 else "lines"
        lines.append("      " + style(f"{style.symbol('ellipsis')} {hidden} more {noun} ({more})", "dim"))
    return lines


def _clip(text: str, full: bool) -> str:
    if full or len(text) <= DIFF_LINE_CHARS:
        return text
    return text[: DIFF_LINE_CHARS - 3] + "..."


def _detail(change: Change, manifest_id: str, style: Style) -> list[str]:
    """Return what an update changes, property by property, then the definition it replaces when read."""
    lines = []
    for item in changed_properties(change, manifest_id):
        before, after = style(item.before or "-", "before"), style(item.after or "-", "after")
        lines.append(f"      {item.name}: {before} {style.symbol('arrow')} {after}")
    if change.action is Action.UPDATE and change.observed is not None and change.observed.definition is not None:
        lines.append("      prior definition:")
        lines += [f"        {line}" for line in change.observed.definition.splitlines()]
    return lines


def _past(outcome: ApplyOutcome) -> str:
    if outcome.status is OutcomeStatus.FAILED:
        return "failed"
    if outcome.status is OutcomeStatus.SKIPPED:
        return "skipped"
    return {Action.CREATE: "created", Action.UPDATE: "updated", Action.PRUNE: "pruned"}.get(outcome.action, "unchanged")


def _cut(cell: str, width: int) -> str:
    if display_width(cell) <= width:
        return cell
    parts = cell.split(".")
    if len(parts) > 2 and " " not in cell:
        for short in (f"{parts[0]}...{parts[-1]}", f"...{parts[-1]}"):
            if display_width(short) <= width:
                return short
    kept = ""
    for char in cell:
        if display_width(kept + char) > width - 3:
            break
        kept += char
    return kept + "..."
