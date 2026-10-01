"""Plans, apply outcomes, and eval runs as `sst` reports them, and each change's statements on disk.

`change_json` and `outcome_json` are what `--output json` lists for each change and each
outcome; `print_plan` and `print_eval_results` are the human reports; `write_plan_sql`
writes what each create or update would execute.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import cast

import click

from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, eval_suite_json
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOutcome, Change, ChangeSet


def change_json(change: Change) -> dict[str, object]:
    """Return one planned change as `--output json` lists it, with its target and fingerprints.

    A prune `sst` will not execute is marked `report_only`.
    """
    rendered = change.rendered
    observed = change.observed
    return {
        "artifact_key": change.key,
        "artifact_type": change.artifact_type,
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
    }


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


def print_plan(changeset: ChangeSet) -> None:
    """Print the plan's counts, then one line per change: its marker, key, action, target, and reason.

    A prune `sst` only reports is counted apart from the prunes and marked `(report only)`.
    """
    counts = {action: sum(change.action is action for change in changeset.changes) for action in Action}
    report_only = len(changeset.report_only)
    click.echo(
        "Plan: "
        f"{counts[Action.CREATE]} to create, {counts[Action.UPDATE]} to update, "
        f"{counts[Action.PRUNE] - report_only} to prune, {counts[Action.NOOP]} unchanged, "
        f"{counts[Action.BLOCKED]} blocked"
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
            else change.observed.qualified_name.sql if change.observed else "-"
        )
        alias = dict(change.rendered.component_fingerprints).get("alias") if change.rendered else None
        click.echo(
            f"{markers[change.action]} {change.key} {change.action.value.upper()} {target} {change.reason.value}"
            + (f" {alias}" if alias else "")
            + (" (report only)" if change.action is Action.PRUNE and not change.prune_executable else "")
        )


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
            else ";\n\n".join(change.rendered.statements) + ";\n"
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
