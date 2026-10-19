"""Plans, apply outcomes, and eval runs as `sst` reports them, and each change's statements on disk.

`change_json` and `outcome_json` are what `--output json` lists for each change and each
outcome; `print_plan`, `print_changed_names` and `print_eval_results` are the human reports;
`write_plan_sql` writes what each create or update would execute; `plan_exit_code` is how a
plan exits.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path
from typing import cast

import click

from snowflake_semantic_tools.adapters.paths import make_folders_within, output_root, remove_within, write_within
from snowflake_semantic_tools.app.baseline import blocking
from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, eval_suite_json
from snowflake_semantic_tools.app.plan import PlanReady
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.output import render_policy
from snowflake_semantic_tools.cli.render.report import plan_lines, table
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOutcome, Change, ChangeSet
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.member_diff import MemberDiff, Value, unified_text
from snowflake_semantic_tools.domain.plan.properties import changed_properties


def plan_exit_code(
    ready: PlanReady, shown: DiagnosticBag, *, detailed: bool, held: frozenset[str] = frozenset()
) -> int:
    """Return how a plan exits: 1 on an error or blocked change, 2 when applying changes Snowflake, else 0.

    An error whose stable fingerprint `held` names is one the run's baseline holds, which
    fails nothing; the plan still exits 2 when it has changes.

    Applying changes Snowflake when the plan writes, and when it only reports prunes that
    state has no entry for yet: apply records them, so the plan is not in sync until it
    runs. Without `detailed`, pending changes exit 0.
    """
    changeset = ready.changeset
    if changeset.blocked or blocking(shown, held):
        return ERROR
    if changeset.writes or ready.restamps_state:
        return CHANGES if detailed else OK
    return OK


def change_json(change: Change, manifest_id: str, *, unread: frozenset[str] = frozenset()) -> dict[str, object]:
    """Return one planned change as `--output json` lists it, with its target and fingerprints.

    A prune `sst` will not execute is marked `report_only`. `properties` names what an update
    changes on the object, publishing under `manifest_id`, and `prior_definition` is the
    object's definition when `--capture-prior` read it, else null. `diff_status` is `compared`
    for an update compared member by member, its members in `diff`; `unavailable` for one in
    `unread`, whose live definition could not be read; `from_state` for one that was not read,
    whose changed members the fingerprints state records name; else `not_compared`. Any update
    not compared lists those members in `changed_members`, when state names them.
    """
    rendered = change.rendered
    observed = change.observed
    return {
        "artifact_key": change.key,
        "artifact_type": change.artifact_type,
        "name": split_artifact_key(change.key)[1],
        "action": change.action.value,
        "reason": change.reason.value,
        "target": (rendered.target.sql if rendered is not None else observed.qualified_name.sql if observed else None),
        "fingerprint": rendered.fingerprint if rendered is not None else None,
        "previous_marker": (observed.marker.text if observed is not None and observed.marker is not None else None),
        "depends_on": list(change.depends_on),
        "order": change.order,
        "component_fingerprints": (dict(rendered.component_fingerprints) if rendered is not None else {}),
        "physical_resources": (
            [
                {"object_type": object_type, "qualified_name": name.sql}
                for object_type, name in rendered.physical_resources
            ]
            if rendered is not None
            else []
        ),
        "prune_executable": change.prune_executable,
        "report_only": change.action is Action.PRUNE and not change.prune_executable,
        "properties": [
            {"property": item.name, "before": item.before, "after": item.after}
            for item in changed_properties(change, manifest_id)
        ],
        "prior_definition": observed.definition if observed is not None else None,
        "diff_status": _diff_status(change, unread),
        "diff": [member_json(item) for item in change.member_diff or ()],
        "changed_members": list(change.changed_members or ()),
    }


def _diff_status(change: Change, unread: frozenset[str]) -> str:
    """Name how an update's members were compared: read, named from state, refused, or not at all."""
    if change.member_diff is not None:
        return "compared"
    if change.key in unread:
        return "unavailable"
    return "from_state" if change.changed_members is not None else "not_compared"


def member_json(item: MemberDiff) -> dict[str, object]:
    """Return one member an update changes as `--output json` lists it: every value in full."""
    return {
        "kind": item.kind,
        "member": item.name,
        "op": item.op,
        "properties": [
            {"property": prop.name, "before": _json_value(prop.before), "after": _json_value(prop.after)}
            for prop in item.properties
        ],
    }


def _json_value(value: Value | None) -> object:
    return list(value) if isinstance(value, tuple) else value


def change_counts(changeset: ChangeSet) -> dict[str, int]:
    """Count a plan's changes by action, a report-only prune counted apart from the prunes."""
    counts = {action.value: sum(change.action is action for change in changeset.changes) for action in Action}
    counts[Action.PRUNE.value] -= len(changeset.report_only)
    counts["report_only"] = len(changeset.report_only)
    return counts


def outcome_json(outcome: ApplyOutcome) -> dict[str, object]:
    """Return one apply outcome as `--output json` lists it."""
    return {
        "artifact_key": outcome.key,
        "action": outcome.action.value,
        "status": outcome.status.value,
        "attempts": outcome.attempts,
        "duration_ms": outcome.duration_ms,
        "grant_check": outcome.grants.value,
        "error": (outcome.error.message if outcome.error else None),
        "component_fingerprints": dict(outcome.component_fingerprints),
    }


def print_plan(
    changeset: ChangeSet, *, full: bool = False, more: str = "--full shows all", unchecked: bool = False
) -> None:
    """Print the plan as `render.report.plan_lines` lays it out: each change, then what stays the same.

    `--verbose` lists the unchanged artifacts too; the verdict says the counts. `more` says where
    the rest of a member diff cut short is. `unchecked` marks the schemas to create as read from
    a recorded observation, not checked now.
    """
    policy = render_policy()
    style = Style(policy.terminal, stdout=True)
    lines = plan_lines(changeset, style, verbose=policy.verbose, full=full, more=more, unchecked=unchecked)
    for line in lines:
        click.echo(line, color=True if policy.terminal.color_out else None)


def print_changed_names(changeset: ChangeSet) -> None:
    """Print the name of each artifact applying the plan would change, one per line, and nothing else.

    Changed means created, updated or pruned, a report-only prune included, in plan order.
    Unchanged and blocked artifacts are not listed.
    """
    for change in changeset.changes:
        if change.action in (Action.CREATE, Action.UPDATE, Action.PRUNE):
            click.echo(split_artifact_key(change.key)[1])


def print_eval_results(result: EvalSuiteResult) -> None:
    """Print every attempt of every eval: its run, status, time and tokens, then each metric's pass rate.

    Status details and a retrieval error follow the attempt they belong to.

    Example:
        eval:jaffle_analyst
          attempt 1  EVAL_RUN_A1  COMPLETED  41.2s  9,812 tokens (6,034 in, 3,778 out)  12 LLM calls
            answer_correctness  5/6 passed  average 0.917
    """
    terminal = render_policy().terminal
    style = Style(terminal, stdout=True)
    for eval_result in result.evals:
        click.echo(style(eval_result.eval_key, "strong"), color=True if terminal.color_out else None)
        for attempt in eval_result.attempts:
            cost = attempt.cost
            tokens = f"{cost.total_tokens:,} tokens ({cost.total_input_tokens:,} in, {cost.total_output_tokens:,} out)"
            row = [f"attempt {attempt.attempt}", attempt.run_name, attempt.terminal_status]
            row += [f"{cost.duration_ms / 1000:.1f}s", tokens, f"{cost.llm_call_count} LLM calls"]
            click.echo("  " + table([row], terminal.width - 2)[0])
            attempt_payload = eval_suite_json(
                dataclasses.replace(result, evals=(dataclasses.replace(eval_result, attempts=(attempt,)),))
            )
            eval_payloads = cast(list[dict[str, object]], attempt_payload["evals"])
            attempt_payloads = cast(list[dict[str, object]], eval_payloads[0]["attempts"])
            summaries = cast(list[dict[str, object]], attempt_payloads[0]["metric_summaries"])
            rows = [
                [
                    str(summary["metric_name"]),
                    f"{summary['passed_count']}/{summary['record_count']} passed",
                    f"average {summary['average_score']}",
                ]
                for summary in summaries
            ]
            for line in table(rows, terminal.width - 4):
                click.echo("    " + line)
            if attempt.status_details:
                click.echo("    status details: " + "; ".join(attempt.status_details))
            if attempt.retrieval_error:
                click.echo(f"    retrieval error: {attempt.retrieval_error}")


def write_plan_sql(project_dir: Path, changeset: ChangeSet, sql_out: Path | None) -> Path:
    """Write what each create or update executes, one file per change; return the directory.

    The directory is `--sql-out`, else `sst_target/sql`. A JSON or YAML payload is written
    as it is; statements are joined, each ending in a semicolon. A file is named by
    `file_name`, so no artifact key can place it outside the directory. A file an earlier run
    wrote there for a change this one does not make is removed, so the directory holds this
    plan's statements only: a regular file named as this names one, `<type>__<name>.<suffix>`.
    Any other file, and any link, is left as it is.

    Raises:
        UnsafeWrite: A symbolic link is on the way to the directory or at a file in it.
        OSError: The directory or a file cannot be written, or a stale file removed.
    """
    output = sql_out or target_dir(project_dir) / "sql"
    root = output_root(project_dir, output)
    make_folders_within(root, output)
    written: set[str] = set()
    for change in changeset.changes:
        if change.rendered is None or change.action not in (
            Action.CREATE,
            Action.UPDATE,
        ):
            continue
        suffix = artifact_suffix(change.rendered.render_dialect)
        stem = f"{change.artifact_type}__{split_artifact_key(change.key)[1].replace('/', '_')}"
        path = output / f"{file_name(stem)}{suffix}"
        content = (
            change.rendered.content
            if suffix in (".json", ".yaml")
            else ";\n\n".join(str(statement) for statement in change.rendered.statements) + ";\n"
        )
        write_within(root, path, content)
        written.add(path.name)
    for stale in _stale_files(output, written):
        remove_within(root, output / stale)
    return output


# A file `write_plan_sql` names: an artifact type, two underscores, a name, and a payload suffix.
_PLAN_FILE = re.compile(r"(?P<type>[a-z_]+)__.+\.(?:sql|json|yaml)")


def _stale_files(output: Path, written: set[str]) -> list[str]:
    """The files in `output` an earlier run wrote and this one did not: regular files, never a link."""
    types = frozenset(SEMANTIC_REGISTRY.artifacts) | {"profile"}
    stale = []
    for entry in sorted(output.iterdir()):
        match = _PLAN_FILE.fullmatch(entry.name)
        if entry.name in written or match is None or match.group("type") not in types:
            continue
        if not entry.is_symlink() and entry.is_file():
            stale.append(entry.name)
    return stale


def plan_diff_path(project_dir: Path, changeset: ChangeSet, diff_out: Path | None) -> Path:
    """Return where a plan writes its member diff: `--diff-out`, else `sst_target/plan.<target>.diff`."""
    return diff_out or target_dir(project_dir) / f"plan.{file_name(changeset.target.name)}.diff"


def plan_diff_text(changeset: ChangeSet, *, plan_id: str, skipped: str | None = None) -> str:
    """Return the plan's member diff as a unified diff: what each update changes, live against desired.

    A header names the target, the plan and when it observed. Each update compared member by
    member is a section; an update that was not, or whose members all agree, is named in a
    comment, which names the members state's fingerprints say it changes when they do; with no
    update, or when `skipped` says why the diffs were not read, the header says so.
    """
    target = changeset.target
    plan = plan_id or "-"
    lines = [f"# sst plan: target {target.name} ({target.scope.sql}), plan {plan}, observed {changeset.observation_at}"]
    updates = [change for change in changeset.changes if change.action is Action.UPDATE]
    if skipped is not None:
        lines.append(f"# member diffs not read: {skipped}")
    elif not updates:
        lines.append("# no updates: nothing to compare")
    for change in updates:
        if change.member_diff is None and change.changed_members:
            names = ", ".join(change.changed_members)
            lines.append(f"# {change.key}: not read; the fingerprints state records name {names}")
        elif skipped is not None:
            continue
        elif change.member_diff is None:
            lines.append(f"# {change.key}: not compared member by member")
        elif not change.member_diff:
            lines.append(f"# {change.key}: definition changed; no member-level difference")
    text = "\n".join(lines) + "\n"
    sections = (
        (change.key, change.rendered.target.sql if change.rendered is not None else "", change.member_diff)
        for change in updates
        if skipped is None and change.member_diff
    )
    body = unified_text(sections)
    return text + ("\n" + body if body else "")


def write_plan_diff(project_dir: Path, path: Path, text: str) -> Path:
    """Write the plan's member diff at `path`, under the project's output root; return the path.

    Raises:
        UnsafeWrite: A symbolic link is on the way to the file or at it.
        OSError: The file cannot be written.
    """
    root = output_root(project_dir, path.parent)
    make_folders_within(root, path.parent)
    write_within(root, path, text)
    return path


def artifact_suffix(render_dialect: str) -> str:
    """Return the file suffix for a payload rendered in `render_dialect`: `.json`, `.yaml`, or `.sql`."""
    if render_dialect in ("json", "bundle_json", "profile_json"):
        return ".json"
    if render_dialect == "eval_yaml":
        return ".yaml"
    return ".sql"
