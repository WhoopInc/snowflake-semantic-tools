"""Pure, deterministic renderers for Cortex Agent evaluation artifacts.

An eval's cases are its dataset's questions, and a case's expectations are the keys of its
ground truth. The renderer expresses `EXPECTATION_KINDS`; any other key passes through as row
metadata, except one in Snowflake's `ground_truth_` vocabulary, which is an expectation the
renderer cannot express (`eval_render_checks`).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalConfig,
    EvalDataset,
    EvalGroundTruth,
    EvalScoreRanges,
    EvalSystemMetric,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.sql import Sql, join, literal, qname, sql

# The ground-truth expectations the renderer writes into a dataset row.
EXPECTATION_KINDS = frozenset({"ground_truth_invocations", "ground_truth_output", "required_filters"})
_EXPECTATION_PREFIX = "ground_truth_"
# An expected tool input that is a query, which a judge can only compare as text.
_SQL_TEXT = re.compile(r"\s*(SELECT|WITH)\b", re.IGNORECASE)


def eval_render_checks(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    """Report each case the renderer cannot express, or expresses as a brittle text match.

    Cases are reported in question order, with their index in the dataset.

    Diagnostics:
        SST-RND021: the dataset has no questions.
        SST-RND020: a question's ground truth names a `ground_truth_` expectation outside
            `EXPECTATION_KINDS`.
        SST-RND022: a question expects a tool input that is SQL text.
    """
    if not resolved.dataset.questions:
        return (D("SST-RND021", subject=resolved.key, artifact=resolved.name),)
    found: list[Diagnostic] = []
    for index, question in enumerate(resolved.dataset.questions):
        truth = question.ground_truth
        if truth is None:
            continue
        found.extend(
            D("SST-RND020", subject=resolved.key, origin=truth.origin, artifact=resolved.name, found=key)
            for key in truth.extra
            if key.startswith(_EXPECTATION_PREFIX) and key not in EXPECTATION_KINDS
        )
        if any(_SQL_TEXT.match(item.tool_input or "") for item in truth.invocations or ()):
            found.append(
                D("SST-RND022", subject=resolved.key, origin=truth.origin, artifact=resolved.name, index=index)
            )
    return tuple(found)


@dataclass(frozen=True, slots=True)
class RenderedEval:
    """Everything one eval publishes, rendered, with the fingerprints that identify it.

    Attributes:
        dataset_payload: The canonical question payload `render_dataset_payload` returns.
        source_table_statements: The statements that create the source table and load the
            payload into it, in order.
        create_dataset_statement: The call that creates the evaluation dataset over the source table.
        config_yaml: The evaluation config that runs against the agent.
        dataset_fingerprint: The hex SHA-256 of `dataset_payload`.
        config_fingerprint: The hex SHA-256 of `config_yaml`.
    """

    dataset_payload: str
    source_table_statements: tuple[Sql, ...]
    create_dataset_statement: Sql
    config_yaml: str
    dataset_fingerprint: str
    config_fingerprint: str

    @property
    def source_table_sql(self) -> str:
        """The source-table statements as one script: each ends in `;`, a blank line between."""
        return source_table_script(self.source_table_statements)


def render_dataset_payload(dataset: EvalDataset) -> str:
    """Return the canonical question payload; coordinates never enter this identity."""
    rows = [
        {
            "ground_truth": _ground_truth_value(question.ground_truth),
            "input_query": question.question or "",
        }
        for question in dataset.questions
    ]
    return json.dumps(rows, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"


def render_source_table_statements(dataset_payload: str, source_table: QualifiedName) -> tuple[Sql, ...]:
    """Render the statements that create the source table and load one row per question into it.

    The table is created, never replaced, and loaded only when there are questions.

    Args:
        dataset_payload: The payload `render_dataset_payload` returned; each ground truth is
            re-encoded compactly with sorted keys.

    Raises:
        ValueError: the payload is not JSON, or a question holds a NUL.
        KeyError: a row lacks `input_query` or `ground_truth`.

    Example:
        CREATE TABLE DB.S.SRC (
            INPUT_QUERY VARCHAR NOT NULL
          , GROUND_TRUTH VARIANT NOT NULL
        );

        INSERT INTO DB.S.SRC (INPUT_QUERY, GROUND_TRUTH)
        SELECT
            'How many orders?'::VARCHAR AS INPUT_QUERY
          , PARSE_JSON('{"ground_truth_output":"42"}')         AS GROUND_TRUTH;
    """
    rows = json.loads(dataset_payload)
    table = qname(source_table)
    statements = [
        sql(
            "CREATE TABLE {table} (\n    INPUT_QUERY VARCHAR NOT NULL\n  , GROUND_TRUTH VARIANT NOT NULL\n)",
            table=table,
        )
    ]
    if rows:
        row_selects = []
        for row in rows:
            ground_truth = json.dumps(
                row["ground_truth"], ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
            row_selects.append(
                sql(
                    "SELECT\n    {question}::VARCHAR AS INPUT_QUERY\n  , PARSE_JSON({truth})         AS GROUND_TRUTH",
                    question=literal(str(row["input_query"])),
                    truth=literal(ground_truth),
                )
            )
        statements.append(
            sql(
                "INSERT INTO {table} (INPUT_QUERY, GROUND_TRUTH)\n{rows}",
                table=table,
                rows=join("\nUNION ALL\n", row_selects),
            )
        )
    return tuple(statements)


def source_table_script(statements: tuple[Sql, ...]) -> str:
    """Write source-table statements as the script the golden holds: each ends in `;`, a blank line apart."""
    return ";\n\n".join(str(statement) for statement in statements) + ";\n"


def render_create_dataset_statement(
    config: EvalConfig,
    source_table: QualifiedName,
    dataset_target: QualifiedName,
) -> Sql:
    """Render the call that creates the evaluation dataset over the source table.

    The source table's question column maps to `query_text`, and its ground truth column to
    `expected_tools`.

    Raises:
        ValueError: the config maps its columns to anything but `input_query` and `ground_truth`
            (in any case), the only columns the source table has.

    Example:
        CALL SYSTEM$CREATE_EVALUATION_DATASET(
            'Cortex Agent'
          , 'DB.S.SRC'
          , 'DB.S.DATASET'
          , OBJECT_CONSTRUCT(
                'query_text', 'INPUT_QUERY'
              , 'expected_tools', 'GROUND_TRUTH'
            )
        )
    """
    columns = config.dataset.column_mapping if config.dataset is not None else None
    if columns is not None and (
        columns.query_text.casefold() != "input_query" or columns.ground_truth.casefold() != "ground_truth"
    ):
        raise ValueError("source-table publication requires input_query and ground_truth column mapping")
    return sql(
        "CALL SYSTEM$CREATE_EVALUATION_DATASET(\n"
        "    'Cortex Agent'\n"
        "  , {source}\n"
        "  , {dataset}\n"
        "  , OBJECT_CONSTRUCT(\n"
        "        'query_text', 'INPUT_QUERY'\n"
        "      , 'expected_tools', 'GROUND_TRUTH'\n"
        "    )\n"
        ")",
        source=literal(source_table.sql),
        dataset=literal(dataset_target.sql),
    )


def render_eval_config(
    config: EvalConfig,
    custom_metrics: tuple[CustomEvalMetric, ...] = (),
    *,
    agent_target: QualifiedName,
    dataset_target: QualifiedName,
) -> str:
    """Emit only Snowflake's closed evaluation and metrics schema; disabled custom metrics are left out."""
    evaluation: dict[str, object] = {
        "agent_params": {
            "agent_name": agent_target.sql,
            "agent_type": "CORTEX AGENT",
            "agent_version": _agent_version(config.agent_version),
        },
    }
    if config.run is not None:
        run_params = {
            key: value
            for key, value in (("label", config.run.label), ("description", config.run.description))
            if value is not None
        }
        if run_params:
            evaluation["run_params"] = run_params
    evaluation["source_metadata"] = {"type": "dataset", "dataset_name": dataset_target.sql}
    metrics: list[object] = [_system_metric_value(metric) for metric in config.system_metrics]
    metrics.extend(_custom_metric_value(metric) for metric in custom_metrics if metric.enabled)
    return _emit_yaml({"evaluation": evaluation, "metrics": metrics})


def _ground_truth_value(value: EvalGroundTruth | None) -> dict[str, object]:
    if value is None:
        return {}
    result: dict[str, object] = {}
    if value.invocations is not None:
        result["ground_truth_invocations"] = [
            {
                key: item
                for key, item in (
                    ("tool_name", invocation.tool_name),
                    ("tool_input", invocation.tool_input),
                    ("tool_output", invocation.tool_output),
                )
                if item is not None
            }
            for invocation in value.invocations
        ]
    if value.output is not None:
        result["ground_truth_output"] = value.output
    if value.required_filters:
        result["required_filters"] = list(value.required_filters)
    result.update(dict(value.extra))
    return result


def _system_metric_value(metric: EvalSystemMetric) -> dict[str, object]:
    return {"name": metric.name or "", "version": metric.version or "v3"}


def _custom_metric_value(metric: CustomEvalMetric) -> dict[str, object]:
    value: dict[str, object] = {"name": metric.name}
    if metric.model is not None:
        value["model"] = metric.model
    if metric.score_ranges is not None:
        value["score_ranges"] = _score_ranges_value(metric.score_ranges)
    if metric.prompt is not None:
        value["prompt"] = metric.prompt
    return value


def _score_ranges_value(value: EvalScoreRanges) -> dict[str, object]:
    return {
        "min_score": list(value.min_score),
        "median_score": list(value.median_score),
        "max_score": list(value.max_score),
    }


def _agent_version(value: str | None) -> str:
    if value is None:
        raise ValueError("eval config requires an immutable agent version")
    if value == "committed":
        return "LAST"
    if value.startswith("alias:"):
        return value.split(":", 1)[1]
    return value


def _emit_yaml(value: Mapping[str, object]) -> str:
    lines: list[str] = []
    _emit_mapping(lines, value, 0)
    return "\n".join(lines) + "\n"


def _emit_mapping(lines: list[str], value: Mapping[str, object], indent: int) -> None:
    for key, item in value.items():
        prefix = " " * indent + f"{key}:"
        if isinstance(item, Mapping):
            lines.append(prefix)
            _emit_mapping(lines, item, indent + 2)
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
            lines.append(prefix)
            _emit_sequence(lines, item, indent + 2)
        elif isinstance(item, str) and "\n" in item:
            lines.append(prefix + " |-")
            lines.extend(" " * (indent + 2) + line if line else "" for line in item.rstrip("\n").split("\n"))
        else:
            lines.append(prefix + " " + _yaml_scalar(item))


def _emit_sequence(lines: list[str], value: Sequence[object], indent: int) -> None:
    for item in value:
        prefix = " " * indent + "-"
        if isinstance(item, Mapping):
            pairs = list(item.items())
            if not pairs:
                lines.append(prefix + " {}")
                continue
            first_key, first_value = pairs[0]
            if isinstance(first_value, (Mapping, list, tuple)) or (
                isinstance(first_value, str) and "\n" in first_value
            ):
                lines.append(prefix)
                _emit_mapping(lines, item, indent + 2)
            else:
                lines.append(prefix + f" {first_key}: {_yaml_scalar(first_value)}")
                _emit_mapping(lines, dict(pairs[1:]), indent + 2)
        else:
            lines.append(prefix + " " + _yaml_scalar(item))


def _yaml_scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Sequence):
        return "[" + ", ".join(_yaml_scalar(item) for item in value) + "]"
    raise TypeError(f"unsupported YAML value {type(value).__name__}")
