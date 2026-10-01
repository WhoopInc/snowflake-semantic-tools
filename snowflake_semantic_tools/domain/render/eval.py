"""Pure, deterministic renderers for Cortex Agent evaluation artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping, Sequence

from ..model.eval import CustomEvalMetric, EvalConfig, EvalDataset, EvalGroundTruth, EvalScoreRanges, EvalSystemMetric
from ..model.identifier import QualifiedName
from ..model.sql import string_literal


@dataclass(frozen=True, slots=True)
class RenderedEval:
    """Everything one eval publishes, rendered, with the fingerprints that identify it.

    Attributes:
        dataset_payload: The canonical question payload `render_dataset_payload` returns.
        source_table_sql: The script that creates the source table and loads the payload into it.
        create_dataset_sql: The call that creates the evaluation dataset over the source table.
        config_yaml: The evaluation config that runs against the agent.
        dataset_fingerprint: The hex SHA-256 of `dataset_payload`.
        config_fingerprint: The hex SHA-256 of `config_yaml`.
    """

    dataset_payload: str
    source_table_sql: str
    create_dataset_sql: str
    config_yaml: str
    dataset_fingerprint: str
    config_fingerprint: str


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


def render_source_table_sql(dataset_payload: str, source_table: QualifiedName) -> str:
    """Render the script that creates the source table and loads one row per question into it.

    The table is created, never replaced, and loaded only when there are questions. Statements
    end with `;` and are separated by a blank line, which is where the caller splits them.

    Args:
        dataset_payload: The payload `render_dataset_payload` returned; each ground truth is
            re-encoded compactly with sorted keys.

    Raises:
        ValueError: the payload is not JSON.
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
    statements = [
        f"CREATE TABLE {source_table.sql} (\n"
        "    INPUT_QUERY VARCHAR NOT NULL\n"
        "  , GROUND_TRUTH VARIANT NOT NULL\n"
        ")"
    ]
    if rows:
        row_selects = []
        for row in rows:
            ground_truth = json.dumps(
                row["ground_truth"], ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
            row_selects.append(
                "SELECT\n"
                f"    {string_literal(str(row['input_query']))}::VARCHAR AS INPUT_QUERY\n"
                f"  , PARSE_JSON({string_literal(ground_truth)})         AS GROUND_TRUTH"
            )
        statements.append(
            f"INSERT INTO {source_table.sql} (INPUT_QUERY, GROUND_TRUTH)\n" + "\nUNION ALL\n".join(row_selects)
        )
    return ";\n\n".join(statements) + ";\n"


def render_create_dataset_sql(
    config: EvalConfig,
    source_table: QualifiedName,
    dataset_target: QualifiedName,
) -> str:
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
        );
    """
    columns = config.dataset.column_mapping if config.dataset is not None else None
    if columns is not None and (
        columns.query_text.casefold() != "input_query" or columns.ground_truth.casefold() != "ground_truth"
    ):
        raise ValueError("source-table publication requires input_query and ground_truth column mapping")
    query_text = "INPUT_QUERY"
    ground_truth = "GROUND_TRUTH"
    return (
        "CALL SYSTEM$CREATE_EVALUATION_DATASET(\n"
        "    'Cortex Agent'\n"
        f"  , {string_literal(source_table.sql)}\n"
        f"  , {string_literal(dataset_target.sql)}\n"
        "  , OBJECT_CONSTRUCT(\n"
        f"        'query_text', {string_literal(query_text)}\n"
        f"      , 'expected_tools', {string_literal(ground_truth)}\n"
        "    )\n"
        ");\n"
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
