"""Plans, apply outcomes, and eval runs as `sst` reports them, and each change's statements on disk.

`change_json` and `outcome_json` are what `--output json` lists for each change and each
outcome; `print_plan`, `print_changed_names` and `print_eval_results` are the human reports;
`write_plan_sql` writes what each create or update would execute; `plan_exit_code` is how a
plan exits.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import cast

import click

from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, eval_suite_json
from snowflake_semantic_tools.app.plan import PlanReady
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOutcome, Change, ChangeSet
from snowflake_semantic_tools.domain.plan.properties import changed_properties


def plan_exit_code(ready: PlanReady, shown: DiagnosticBag, *, detailed: bool) -> int:
    """Return how a plan exits: 1 on an error or blocked change, 2 when applying changes Snowflake, else 0.

    Applying changes Snowflake when the plan writes, and when it only reports prunes that
    state has not yet recorded under this manifest: apply re-stamps state for them, so the
    plan is not in sync until it runs. Without `detailed`, pending changes exit 0.
    """
    changeset = ready.changeset
    if changeset.blocked or shown.has_errors:
        return ERROR
    if changeset.writes or ready.restamps_state:
        return CHANGES if detailed else OK
    return OK


def change_json(change: Change, manifest_id: str) -> dict[str, object]:
    """Return one planned change as `--output json` lists it, with its target and fingerprints.

    A prune `sst` will not execute is marked `report_only`. `properties` names what an update
    changes on the object, publishing under `manifest_id`, and `prior_definition` is the
    object's definition when `--capture-prior` read it, else null.
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
    }


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


def print_plan(changeset: ChangeSet, *, full: bool = False) -> None:
    """Print the plan's counts, then one line per change: its marker, key, action, target, and reason.

    A prune `sst` only reports is counted apart from the prunes and marked `(report only)`.
    With `full`, each update is followed by the properties it changes and, when
    `--capture-prior` read it, the definition it replaces.
    """
    counts = change_counts(changeset)
    report_only = counts["report_only"]
    click.echo(
        "Plan: "
        f"{counts['create']} to create, {counts['update']} to update, "
        f"{counts['prune']} to prune, {counts['noop']} unchanged, "
        f"{counts['blocked']} blocked"
        + (f", {report_only} report-only (SST never removes these)." if report_only else ".")
    )
    markers = {
        Action.CREATE: "+",
        Action.UPDATE: "~",
        Action.PRUNE: "-",
        Action.NOOP: "=",
        Action.BLOCKED: "!",
    }
    for change in changeset.changes:
        target = (
            change.rendered.target.sql
            if change.rendered
            else change.observed.qualified_name.sql
            if change.observed
            else "-"
        )
        alias = dict(change.rendered.component_fingerprints).get("alias") if change.rendered else None
        click.echo(
            f"{markers[change.action]} {change.key} {change.action.value.upper()} {target} {change.reason.value}"
            + (f" {alias}" if alias else "")
            + (" (report only)" if change.action is Action.PRUNE and not change.prune_executable else "")
        )
        if full:
            _print_detail(change, changeset.manifest_id)


def _print_detail(change: Change, manifest_id: str) -> None:
    """Print what an update changes, property by property, then the definition it replaces when read."""
    for item in changed_properties(change, manifest_id):
        click.echo(f"    {item.name}: {item.before or '-'} -> {item.after or '-'}")
    if change.observed is not None and change.observed.definition is not None:
        click.echo("    prior definition:")
        for line in change.observed.definition.splitlines():
            click.echo(f"      {line}")


def print_changed_names(changeset: ChangeSet) -> None:
    """Print the name of each artifact applying the plan would change, one per line, and nothing else.

    Changed means created, updated or pruned, a report-only prune included, in plan order.
    Unchanged and blocked artifacts are not listed.
    """
    for change in changeset.changes:
        if change.action in (Action.CREATE, Action.UPDATE, Action.PRUNE):
            click.echo(split_artifact_key(change.key)[1])


def print_eval_results(result: EvalSuiteResult) -> None:
    """Print every attempt of every eval: its run, status and cost, then each metric's pass rate.

    Status details and a retrieval error follow the attempt they belong to.
    """
    for eval_result in result.evals:
        click.echo(f"{eval_result.eval_key}:")
        for attempt in eval_result.attempts:
            click.echo(
                f"  attempt {attempt.attempt}: {attempt.run_name} {attempt.terminal_status} "
                f"duration_ms={attempt.cost.duration_ms} tokens={attempt.cost.total_tokens} "
                f"agent_input={attempt.cost.total_input_tokens} agent_output={attempt.cost.total_output_tokens} "
                f"llm_calls={attempt.cost.llm_call_count}"
            )
            attempt_payload = eval_suite_json(
                dataclasses.replace(result, evals=(dataclasses.replace(eval_result, attempts=(attempt,)),))
            )
            eval_payloads = cast(list[dict[str, object]], attempt_payload["evals"])
            attempt_payloads = cast(list[dict[str, object]], eval_payloads[0]["attempts"])
            summaries = cast(list[dict[str, object]], attempt_payloads[0]["metric_summaries"])
            for summary in summaries:
                click.echo(
                    f"    {summary['metric_name']}: passed={summary['passed_count']}/{summary['record_count']} "
                    f"average={summary['average_score']}"
                )
            if attempt.status_details:
                click.echo("    status_details: " + "; ".join(attempt.status_details))
            if attempt.retrieval_error:
                click.echo(f"    retrieval_error: {attempt.retrieval_error}")


def write_plan_sql(project_dir: Path, changeset: ChangeSet, sql_out: Path | None) -> Path:
    """Write what each create or update executes, one file per change; return the directory.

    The directory is `--sql-out`, else `target/sst/sql`. A JSON or YAML payload is written
    as it is; statements are joined, each ending in a semicolon.
    """
    output = sql_out or target_dir(project_dir) / "sql"
    output.mkdir(parents=True, exist_ok=True)
    for change in changeset.changes:
        if change.rendered is None or change.action not in (
            Action.CREATE,
            Action.UPDATE,
        ):
            continue
        suffix = artifact_suffix(change.rendered.render_dialect)
        path = output / f"{change.artifact_type}__{split_artifact_key(change.key)[1].replace('/', '_')}{suffix}"
        content = (
            change.rendered.content
            if suffix in (".json", ".yaml")
            else ";\n\n".join(str(statement) for statement in change.rendered.statements) + ";\n"
        )
        path.write_text(content, encoding="utf-8")
    return output


def artifact_suffix(render_dialect: str) -> str:
    """Return the file suffix for a payload rendered in `render_dialect`: `.json`, `.yaml`, or `.sql`."""
    if render_dialect in ("json", "bundle_json", "profile_json"):
        return ".json"
    if render_dialect == "eval_yaml":
        return ".yaml"
    return ".sql"
