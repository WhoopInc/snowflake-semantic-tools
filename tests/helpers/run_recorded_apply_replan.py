#!/usr/bin/env python3
"""Apply a recorded clean-target plan, then emit its zero-write re-plan."""

from __future__ import annotations

import argparse
import json
import pathlib
import tempfile
from collections.abc import Callable, Sequence

from click.testing import CliRunner
from run_recorded_plan import recorded
from snowflake_fake import Sent

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.cli import main as cli_module
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, OwnershipMarker, QueryResult, ShowRow
from snowflake_semantic_tools.domain.sql import Sql

OBJECT_TYPES = {
    "semantic_view": "SEMANTIC VIEW",
    "tool": "CORTEX SEARCH SERVICE",
    "agent": "AGENT",
}
# Composite artifacts carry no COMMENT marker and are not SHOW-observed; the
# recorded adapter keeps their state (versions, trees, registry rows) itself.
COMPOSITE_TYPES = frozenset(("eval", "skill", "plugin", "profile"))


def invoke(runner: CliRunner, args: list[str], expected_exit: int) -> dict[str, object]:
    result = runner.invoke(cli_module.cli, args)
    if result.exit_code != expected_exit:
        raise RuntimeError(
            f"sst {' '.join(args[:2])} exited {result.exit_code}, expected {expected_exit}:\n{result.output}"
        )
    payload = json.loads(result.output)
    if not isinstance(payload, dict):
        raise RuntimeError("SST did not emit a JSON object")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("observation", type=pathlib.Path)
    parser.add_argument("--project-dir", type=pathlib.Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()
    manifest_path = args.manifest.resolve()
    port = recorded(args.observation)
    port.close = lambda: None
    # Swap the connector class for a factory returning the recorded double.
    cli_module.SnowflakeConnector = lambda params: port  # type: ignore[assignment, misc]
    common = [
        "--project-dir",
        str(project_dir),
        "--target",
        args.target,
        "--manifest",
        str(manifest_path),
        "--no-strict",
        "--no-snowflake-syntax-check",
    ]
    runner = CliRunner()

    with tempfile.TemporaryDirectory(prefix="sst-recorded-apply-") as raw_temp:
        temp = pathlib.Path(raw_temp)
        plan_path = temp / "plan.json"
        first = invoke(
            runner,
            [
                "plan",
                *common,
                "--plan-out",
                str(plan_path),
                "--sql-out",
                str(temp / "create-sql"),
                "--output",
                "json",
            ],
            2,
        )
        data = first.get("data")
        changes = data.get("changes") if isinstance(data, dict) else None
        manifest_id = data.get("manifest_id") if isinstance(data, dict) else None
        if not isinstance(changes, list) or not isinstance(manifest_id, str):
            raise RuntimeError("create plan omitted changes or manifest identity")

        compiled = compile_result(locate_project(project_dir), args.target, manifest_path)
        port.existing = {
            relation.sql for item in compiled.compiled for relation in item.rendered_artifact.required_relations
        }
        row_counts = {
            item.source_table.sql: len(item.resolved.dataset.questions)
            for item in compiled.compiled
            if isinstance(item, CompiledEval)
        }
        original_query: Callable[[Sql, object], QueryResult] = port.query

        def query(sql: Sql, params: object = None) -> QueryResult:
            prefix = "SELECT COUNT(*) AS ROW_COUNT FROM "
            text = str(sql)
            if text.startswith(prefix) and text[len(prefix) :] in row_counts:
                port.log.append(Sent("query", (text,), params))
                return QueryResult(("ROW_COUNT",), ((row_counts[text[len(prefix) :]],),))
            return original_query(sql, params)

        port.query = query
        original_execute: Callable[[Sequence[Sql]], ExecResult] = port.execute_script

        def execute_with_markers(statements: Sequence[Sql]) -> ExecResult:
            result = original_execute(statements)
            if not result.ok:
                return result
            sql = "\n".join(str(statement) for statement in statements)
            for raw in changes:
                if not isinstance(raw, dict) or raw.get("artifact_type") in COMPOSITE_TYPES:
                    continue
                target = str(raw.get("target") or "")
                if target and target in sql:
                    port.markers[target] = OwnershipMarker(
                        manifest_id,
                        str(raw.get("fingerprint") or ""),
                    )
            return result

        port.execute_script = execute_with_markers
        applied = invoke(
            runner,
            [
                "apply",
                *common,
                "--plan",
                str(plan_path),
                "--yes",
                "--sql-out",
                str(temp / "apply-sql"),
                "--output",
                "json",
            ],
            0,
        )
        applied_data = applied.get("data")
        if not isinstance(applied_data, dict) or applied_data.get("state_written") is not True:
            raise RuntimeError("recorded apply did not persist authoritative state")

        rows: dict[tuple[str, str], list[ShowRow]] = {}
        for raw in changes:
            if not isinstance(raw, dict):
                continue
            artifact_type = str(raw.get("artifact_type") or "")
            if artifact_type in COMPOSITE_TYPES:
                continue
            object_type = OBJECT_TYPES[artifact_type]
            target = str(raw["target"])
            database, schema, name = target.split(".")
            rows.setdefault((object_type, f"{database}.{schema}"), []).append(
                ShowRow(
                    name,
                    database,
                    schema,
                    "RECORDED_OWNER",
                    "2026-01-01T00:00:00Z",
                    f"[sst:{manifest_id}:{raw['fingerprint']}]",
                    object_type=object_type,
                )
            )
        port.objects = {key: tuple(value) for key, value in rows.items()}

        scripts_before = len(port.scripts)
        uploads_before = len(port.uploads)
        replanned = invoke(
            runner,
            [
                "plan",
                *common,
                "--no-plan-out",
                "--sql-out",
                str(temp / "replan-sql"),
                "--output",
                "json",
            ],
            0,
        )
        replan_data = replanned.get("data")
        replan_changes = replan_data.get("changes") if isinstance(replan_data, dict) else None
        if not isinstance(replan_changes, list) or {
            item.get("action") for item in replan_changes if isinstance(item, dict)
        } != {"noop"}:
            raise RuntimeError("recorded apply did not re-plan to all NOOP changes")
        if len(port.scripts) != scripts_before or len(port.uploads) != uploads_before:
            raise RuntimeError("repeat planning wrote to the recorded Snowflake adapter")
        print(json.dumps(replanned, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
