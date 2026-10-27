"""The `agent` output format: a short, fixed-grammar text document for a coding agent to read.

An agent reads text natively and pays for every token, so the document carries what it needs to
act and nothing else, in a grammar a line-by-line reader can rely on:

- the first line is the verdict, `sst <command>: <OUTCOME> key=value ...`;
- each finding is one line, `E`, `W` or `I`, then its code, location, subject and message,
  with a `fix:` line under it and the follow-on findings it caused as `cascade` lines;
- a plan's changes, apply's outcomes, list's items, the kit skills `agent-kit status` finds
  not current, and the files and entries `format`, `clean`, `migrate`, and `baseline` change
  or list follow, one line each, marked `+ ~ - ! =` where a line adds, changes, removes, leaves
  alone, or keeps something;
- the last line is `next:`, which never tells an agent to change Snowflake on its own.

Its own marks are ASCII, and it has no color, progress, links, info findings, or baselined
ones (both are counted in the verdict); a project's own text is kept as written, control
characters replaced. It stops at `MAX_LINES`, the last line then saying how many were left out.
`--output json` has every field; this format is for reading, not parsing.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, CONNECTION, ERROR, INTERRUPTED, OK, USAGE
from snowflake_semantic_tools.cli.render.diagnostics import located_message, location
from snowflake_semantic_tools.cli.render.frames import HINT_KEYS, pinpoint
from snowflake_semantic_tools.cli.render.report import property_labels
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Severity

MAX_LINES = 200
# The most members a changed line names; the rest are counted.
_MAX_MEMBERS = 6
_OUTCOMES = {
    OK: "PASS",
    CHANGES: "CHANGES",
    ERROR: "FAIL",
    USAGE: "USAGE",
    CONFIG: "CONFIG",
    CONNECTION: "CONNECTION",
    INTERRUPTED: "INTERRUPTED",
}
_LETTERS = {Severity.ERROR: "E", Severity.WARNING: "W", Severity.INFO: "I"}
_MARKS = {"create": "+", "update": "~", "prune": "-", "blocked": "!", "noop": "="}


def agent_document(
    command: str,
    exit_code: int,
    diagnostics: DiagnosticBag,
    *,
    baselined: frozenset[str],
    fingerprint: Callable[[Diagnostic], str],
    project_dir: Path,
    data: object = None,
    seconds: float | None = None,
    note: str | None = None,
    next_steps: Sequence[str] = (),
) -> str:
    """Return the agent document for one run, ending in a newline.

    Args:
        baselined: The stable fingerprints the run's baseline holds; those findings are counted.
        fingerprint: Returns a diagnostic's stable fingerprint, as `baseline` matches it.
        data: The envelope's `data`; a plan's `changes`, apply's `outcomes`, list's `items` and
            the skills `agent-kit status` finds not current are listed from it.
        note: Why the format was chosen, when the run did not ask for it.

    Example:
        sst validate: FAIL exit=1 errors=1 warnings=0 baselined=8 info_hidden=166 time=0.9s
        E SST-REF002 metrics/customer_count.yml:5:46 metric:customer_count | 'customer_uuid' is not a column
          fix: correct the column name, or add it to the model's columns: block
        next: fix the error, then run `sst validate` again
    """
    findings, counts = _findings(diagnostics, baselined, fingerprint, project_dir)
    payload = data if isinstance(data, Mapping) else {}
    lines = [_verdict(command, exit_code, counts, payload, seconds, note)]
    body = [*findings, *_body(command, payload)]
    room = MAX_LINES - 2
    if len(body) > room:
        left = len(body) - (room - 1)
        body = [*body[: room - 1], f"... {left} more lines left out; `sst -o json {command}` has every one"]
    lines += body
    lines.append(f"next: {'; '.join(next_steps)}" if next_steps else "next: nothing to do")
    return "\n".join(_printable(line) for line in lines) + "\n"


def _findings(
    diagnostics: DiagnosticBag, baselined: frozenset[str], fingerprint: Callable[[Diagnostic], str], project_dir: Path
) -> tuple[list[str], dict[str, int]]:
    """Return the finding lines, a cascade under its cause, and what the verdict counts."""
    held = [item for item in diagnostics if fingerprint(item) in baselined]
    live = [item for item in diagnostics if item not in held]
    cascades = [item for item in live if item.cascaded]
    shown = [item for item in live if not item.cascaded and not item.informational]
    shown.sort(key=lambda item: not item.blocks)
    counts = {
        "errors": sum(item.blocks for item in live),
        "warnings": sum(not item.blocks and not item.informational for item in live),
        "baselined": len(held),
        "info_hidden": sum(item.informational and not item.cascaded for item in live),
        "cascade_hidden": 0,
    }
    placed: set[int] = set()
    lines: list[str] = []
    for item in shown:
        lines.append(_finding(item, project_dir))
        if item.suggestion:
            lines.append(f"  fix: {_one_line(item.suggestion)}")
        for index, cascade in enumerate(cascades):
            if index not in placed and cascade.caused_by == item.code and cascade.subject in (item.subject, None):
                placed.add(index)
                lines.append(f"  cascade {cascade.code} {_where(cascade, project_dir)}| {_one_line(cascade.message)}")
    counts["cascade_hidden"] = len(cascades) - len(placed)
    return lines, counts


def _finding(item: Diagnostic, project_dir: Path) -> str:
    subject = f"{item.subject} " if item.subject and item.subject != "cli" else ""
    message = _one_line(located_message(item))
    # Many messages open with their subject, which the line has just given.
    if item.subject and message.startswith(f"{item.subject}: "):
        message = message.removeprefix(f"{item.subject}: ")
    return f"{_LETTERS[item.severity]} {item.code} {_where(item, project_dir)}{subject}| {message}"


def _where(item: Diagnostic, project_dir: Path) -> str:
    """Return the finding's location and a space, moved onto the value it names; empty with none."""
    if item.origin is None:
        return ""
    hints = [str(item.context[key]) for key in HINT_KEYS if item.context.get(key) is not None]
    return location(pinpoint(project_dir, item.origin, hints)) + " "


def _verdict(
    command: str,
    exit_code: int,
    counts: Mapping[str, int],
    data: Mapping[str, object],
    seconds: float | None,
    note: str | None,
) -> str:
    """Return the first line: the outcome, the exit code, the counts, a plan's changes, the time."""
    pairs = [
        f"exit={exit_code}",
        *(f"{key}={value}" for key, value in counts.items() if value or key in ("errors", "warnings")),
    ]
    plan_counts = data.get("counts")
    if command == "plan" and isinstance(plan_counts, Mapping):
        for key in ("create", "update", "prune", "blocked", "noop"):
            pairs.append(f"{'unchanged' if key == 'noop' else key}={plan_counts.get(key, 0)}")
    if seconds is not None:
        pairs.append(f"time={seconds:.1f}s")
    if note:
        pairs.append(f"output=agent({note})")
    return f"sst {command}: {_OUTCOMES.get(exit_code, 'FAIL')} {' '.join(pairs)}"


def _body(command: str, data: Mapping[str, object]) -> list[str]:
    """Return the lines a command adds after its findings: changes, outcomes, or listed items."""
    lines: list[str] = []
    for change in _rows(data.get("changes")):
        if change.get("action") == "noop":
            continue
        names = [str(item.get("property")) for item in _rows(change.get("properties"))]
        props = [label.replace(" ", "_") for label in property_labels(names)]
        if change.get("diff_status") == "compared" and "definition" in props:
            at = props.index("definition")
            props[at : at + 1] = _members(_rows(change.get("diff"))) or ["definition"]
        elif change.get("diff_status") == "from_state" and "definition" in props:
            at = props.index("definition")
            props[at : at + 1] = _changed(change.get("changed_members")) or ["definition"]
        detail = f" changes={','.join(props)}" if props else ""
        only = " report_only" if change.get("report_only") else ""
        mark = _MARKS.get(str(change.get("action")), "?")
        # A report-only prune of an object nothing observed has no target, as in the table.
        target = change.get("target") or "-"
        lines.append(f"{mark} {change.get('artifact_key')} {target} {change.get('reason')}{detail}{only}")
    for outcome in _rows(data.get("outcomes")):
        if outcome.get("action") == "noop":
            continue
        error = f" | {_one_line(str(outcome['error']))}" if outcome.get("error") else ""
        lines.append(f"{outcome.get('status')} {outcome.get('artifact_key')} {outcome.get('action')}{error}")
    return [*lines, *_listed(command, data)]


def _listed(command: str, data: Mapping[str, object]) -> list[str]:
    """Return one line per item `list` lists, node `lineage` traces, kit skill not current, or file
    and baseline entry a command changes or lists."""
    if command in _FILE_COMMANDS:
        return _files(command, data)
    if command == "list":
        listed = ("key", "status", "target", "relation")
        return [" ".join(str(item.get(key)) for key in listed if item.get(key)) for item in _rows(data.get("items"))]
    if command == "agent-kit status":
        return _kit_skills(data)
    if command != "lineage":
        return []
    traced = ("direction", "depth", "key", "kind", "relation")
    return [
        " ".join(str(item.get(key)) for key in traced if item.get(key) is not None)
        + (" changed" if item.get("changed") else "")
        + (f" | {item['marker']}" if item.get("marker") else "")
        for item in _rows(data.get("nodes"))
    ]


# The commands whose files or baseline entries `_files` lists.
_FILE_COMMANDS = frozenset(
    ("format", "clean", "migrate refs", "migrate split", "baseline show", "baseline add", "baseline prune")
)


def _files(command: str, data: Mapping[str, object]) -> list[str]:
    """Return a line per file a command changes or would, per place it left alone, or per baseline entry.

    `~` a file rewritten, or that would be; `-` one removed, or an entry pruned; `+` an entry
    added; `!` a file or reference left as it is, with why; `=` an entry `baseline show` lists.
    """
    lines = [f"~ {path}" for path in _strings(data.get("would_change"))]
    lines += [f"~ {path} formatted" for path in _strings(data.get("formatted"))]
    lines += [f"- {path} would be removed" for path in _strings(data.get("would_remove"))]
    lines += [f"- {path} removed" for path in _strings(data.get("removed"))]
    for item in _rows(data.get("files")):
        path = item.get("path")
        if item.get("refused"):
            lines.append(f"! {path} left whole | {_one_line(str(item['refused']))}")
        elif item.get("into"):
            lines.append(f"~ {path} into {','.join(_strings(item.get('into')))}")
        rewrites = item.get("rewrites")
        if isinstance(rewrites, Mapping) and any(rewrites.values()):
            counts = ",".join(f"{kind}={value}" for kind, value in rewrites.items() if value)
            lines.append(f"~ {path} {counts}")
        for left in _rows(item.get("untouched")):
            where = f"{path}:{left.get('line')}:{left.get('column')}"
            lines.append(f"! {where} {left.get('text')} | {_one_line(str(left.get('reason')))}")
    config = data.get("config")
    if isinstance(config, Mapping):
        for rewrite in _rows(config.get("rewrites")):
            change = f"{rewrite.get('before') or '(new)'} -> {rewrite.get('after') or '(removed)'}"
            lines.append(f"~ {config.get('path')}:{rewrite.get('line')} {change}")
        for left in _rows(config.get("untouched")):
            lines.append(f"! {config.get('path')}:{left.get('line')} {left.get('text')} | {left.get('reason')}")
    marks = (("added", "+"), ("pruned", "-")) + ((("entries", "="),) if command == "baseline show" else ())
    for key, mark in marks:
        for entry in _rows(data.get(key)):
            where = " ".join(str(entry[name]) for name in ("artifact", "file") if entry.get(name))
            lines.append(f"{mark} {entry.get('code')} {where}".rstrip())
    return lines


def _strings(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _kit_skills(data: Mapping[str, object]) -> list[str]:
    """Return `<state> <agent> <skill>` per kit skill that is outdated, missing or modified."""
    return [
        f"{skill.get('state')} {agent.get('agent')} {skill.get('name')}"
        for agent in _rows(data.get("agents"))
        for skill in _rows(agent.get("skills"))
        if skill.get("state") not in ("current", "not installed")
    ]


def _members(diff: list[Mapping[str, object]]) -> list[str]:
    """Name each member an update changes as `kind:name` and `~`, `+` or `-`, at most `_MAX_MEMBERS`."""
    marks = {"changed": "~", "added": "+", "removed": "-"}
    names = [
        _printable(f"{item.get('kind')}:{item.get('member')}{marks.get(str(item.get('op')), '~')}").replace(" ", "_")
        for item in diff
    ]
    more = len(names) - _MAX_MEMBERS
    return names[:_MAX_MEMBERS] + ([f"{more}_more"] if more > 0 else [])


def _changed(keys: object) -> list[str]:
    """Name each member state's fingerprints say an update changes as `kind:name~`, at most `_MAX_MEMBERS`."""
    names = [_printable(f"{key}~").replace(" ", "_") for key in keys] if isinstance(keys, list) else []
    more = len(names) - _MAX_MEMBERS
    return names[:_MAX_MEMBERS] + ([f"{more}_more"] if more > 0 else [])


def _rows(value: object) -> list[Mapping[str, object]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _printable(text: str) -> str:
    """Replace control characters, so no project text can end a line early or drive a terminal."""
    return "".join("?" if unicodedata.category(char) in ("Cc", "Cf") else char for char in text)
