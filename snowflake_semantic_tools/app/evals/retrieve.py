"""Read what an evaluation run reports: its status while it runs, and its results once it completes.

Nothing here trusts a row it has not checked. A status row must name the run and the agent
it was asked for; the results must answer exactly the compiled dataset's questions, each with
exactly the configured metrics, one record per question. A mismatch raises, and the run
records it as the attempt's retrieval error.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from hashlib import sha256

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.domain.model.eval import EvalCostSummary, EvalMetricResult, EvalResultRow, ThresholdRange
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import sql

_STATUS_COLUMNS = ("RUN_NAME", "AGENT_NAME", "AGENT_TYPE", "STATUS", "STATUS_DETAILS")
_RESULT_COLUMNS = (
    "RECORD_ID",
    "INPUT_ID",
    "REQUEST_ID",
    "TIMESTAMP",
    "DURATION_MS",
    "INPUT",
    "OUTPUT",
    "ERROR",
    "GROUND_TRUTH",
    "METRIC_NAME",
    "EVAL_AGG_SCORE",
    "METRIC_TYPE",
    "METRIC_STATUS",
    "METRIC_CALLS",
    "TOTAL_INPUT_TOKENS",
    "TOTAL_OUTPUT_TOKENS",
    "LLM_CALL_COUNT",
)
_STATUS_CALL = sql("CALL EXECUTE_AI_EVALUATION('STATUS', OBJECT_CONSTRUCT('run_name', %s), %s)")
_RESULTS_QUERY = sql("SELECT * FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(%s, %s, %s, %s, %s))")

_RecordQuestion = tuple[str, str]


def _read_status(
    port: ExecutionPort,
    compiled: CompiledEval,
    run_name: str,
    config_path: str,
) -> tuple[str, tuple[str, ...]]:
    """Read one run's status, uppercased, and its details, from the row that describes this run.

    Raises:
        SnowflakePortError: the read failed, it did not return exactly one row, or the row
            describes another run, agent or agent type.
        ValueError: the row omits a column or a value, or its details are neither an array nor text.
    """
    result = port.query_in_context(
        SchemaScope(compiled.agent_target.database, compiled.agent_target.schema),
        _STATUS_CALL,
        (run_name, config_path),
    )
    row = _single_row(result.columns, result.rows, _STATUS_COLUMNS, "evaluation status")
    if _required_text(row, "RUN_NAME") != run_name:
        raise SnowflakePortError(f"evaluation status returned run {_required_text(row, 'RUN_NAME')!r}")
    if _required_text(row, "AGENT_NAME").casefold() != compiled.agent_target.artifact_name:
        raise SnowflakePortError("evaluation status returned a different agent")
    if _required_text(row, "AGENT_TYPE").upper() != "CORTEX AGENT":
        raise SnowflakePortError("evaluation status returned a different agent type")
    return _required_text(row, "STATUS").upper(), _status_details(row.get("STATUS_DETAILS"))


def _read_results(
    port: ExecutionPort,
    compiled: CompiledEval,
    run_name: str,
) -> tuple[tuple[EvalResultRow, ...], EvalCostSummary]:
    """Read a completed run's results: one row per question, in record order, and the run's cost.

    Raises:
        SnowflakePortError: the read failed.
        ValueError: the results are empty, malformed, or do not match the compiled eval, as
            `_ResultCollector` checks them.
    """
    result = port.query_in_context(
        SchemaScope(compiled.agent_target.database, compiled.agent_target.schema),
        _RESULTS_QUERY,
        (
            compiled.agent_target.database.folded,
            compiled.agent_target.schema.folded,
            compiled.agent_target.name.folded,
            "CORTEX AGENT",
            run_name,
        ),
    )
    records = _rows_by_name(result.columns, result.rows, _RESULT_COLUMNS, "evaluation results")
    if not records:
        raise ValueError(f"evaluation run {run_name!r} returned no result rows")
    collector = _ResultCollector(compiled, _expected_question_map(compiled))
    for record in records:
        collector.add(record)
    collector.check_complete()
    return collector.rows(), collector.cost()


class _ResultCollector:
    """Group a run's result records into one row per question, checking each as it is added.

    A record is one metric's result on one question. A record id answers one question and a
    question has one record id; a metric appears once per record, with the type the config
    gives it. The cost counts each request once, however many metric rows repeat it.
    """

    def __init__(
        self,
        compiled: CompiledEval,
        expected_questions: Mapping[str, tuple[str, Mapping[str, object]]],
    ) -> None:
        self._compiled = compiled
        self._expected_questions = expected_questions
        self._expected_metrics = _expected_metrics(compiled)
        self._grouped: dict[_RecordQuestion, list[EvalMetricResult]] = {}
        self._costs: dict[str, EvalCostSummary] = {}
        self._inputs: dict[_RecordQuestion, str] = {}
        self._seen_metrics: set[tuple[str, str, str]] = set()
        self._record_questions: dict[str, str] = {}
        self._question_records: dict[str, str] = {}

    def add(self, record: Mapping[str, object]) -> None:
        """Add one record's metric result and cost, refusing one that contradicts what came before.

        Raises:
            ValueError: the record names no known question or metric, repeats a metric, pairs a
                record id and a question differently than before, or omits a required value.
        """
        question_key = _result_question_key(record, self._expected_questions)
        record_id = _required_text(record, "RECORD_ID")
        self._pair(record_id, question_key)
        group_key = (record_id, question_key)
        metric_name = self._checked_metric(record, record_id)
        score = _optional_float(record.get("EVAL_AGG_SCORE"))
        passed = _metric_passed(
            self._compiled,
            metric_name,
            score,
            record.get("METRIC_STATUS"),
            record.get("ERROR"),
        )
        self._grouped.setdefault(group_key, []).append(EvalMetricResult(question_key, metric_name, score, passed))
        self._record_input(group_key, record_id, str(record.get("INPUT") or ""))
        request_id = _required_text(record, "REQUEST_ID")
        self._costs[request_id] = _merge_record_cost(self._costs.get(request_id), _row_cost(record))

    def check_complete(self) -> None:
        """Refuse results whose questions, or any question's metrics, differ from the compiled eval.

        Raises:
            ValueError: a question of the dataset has no results or one outside it has some, or
                a question's metrics differ from the configured ones.
        """
        expected_question_keys = frozenset(question_key for question_key, _ in self._expected_questions.values())
        found_questions = frozenset(self._question_records)
        if found_questions != expected_question_keys:
            raise ValueError(
                "evaluation results question set differs from the authored dataset: "
                f"missing={sorted(expected_question_keys - found_questions)!r}, "
                f"unexpected={sorted(found_questions - expected_question_keys)!r}"
            )
        expected_metric_names = frozenset(self._expected_metrics)
        for (_, question_key), metrics in self._grouped.items():
            found_metric_names = frozenset(metric.metric_name.casefold() for metric in metrics)
            if found_metric_names != expected_metric_names:
                raise ValueError(
                    f"evaluation question {question_key!r} metric set differs from config: "
                    f"missing={sorted(expected_metric_names - found_metric_names)!r}, "
                    f"unexpected={sorted(found_metric_names - expected_metric_names)!r}"
                )

    def rows(self) -> tuple[EvalResultRow, ...]:
        """Return one row per question, ordered by record id, with its metrics ordered by name."""
        return tuple(
            EvalResultRow(
                question_key,
                self._inputs[group_key],
                tuple(sorted(metrics, key=lambda item: item.metric_name)),
            )
            for group_key, metrics in sorted(self._grouped.items(), key=lambda item: item[0])
            for question_key in (group_key[1],)
        )

    def cost(self) -> EvalCostSummary:
        """Return the run's cost: the sum over its requests."""
        return _sum_costs(tuple(self._costs.values()))

    def _pair(self, record_id: str, question_key: str) -> None:
        """Pair a record id with its question, refusing a record id or a question paired otherwise."""
        if record_id in self._record_questions and self._record_questions[record_id] != question_key:
            raise ValueError(f"evaluation record {record_id!r} maps to multiple questions")
        if question_key in self._question_records and self._question_records[question_key] != record_id:
            raise ValueError(f"evaluation question {question_key!r} maps to multiple records")
        self._record_questions[record_id] = question_key
        self._question_records[question_key] = record_id

    def _checked_metric(self, record: Mapping[str, object], record_id: str) -> str:
        """Return the record's metric name, once it is configured, of its type, and new to the record."""
        metric_name = _required_text(record, "METRIC_NAME")
        metric_type = _required_text(record, "METRIC_TYPE").casefold()
        expected_type = self._expected_metrics.get(metric_name.casefold())
        if expected_type is None:
            raise ValueError(f"evaluation results returned unknown metric {metric_name!r}")
        if metric_type != expected_type:
            raise ValueError(f"evaluation metric {metric_name!r} has type {metric_type!r}, expected {expected_type!r}")
        duplicate_key = (record_id, metric_name.casefold(), metric_type)
        if duplicate_key in self._seen_metrics:
            raise ValueError(f"evaluation results contain duplicate metric row {duplicate_key!r}")
        self._seen_metrics.add(duplicate_key)
        return metric_name

    def _record_input(self, group_key: _RecordQuestion, record_id: str, input_query: str) -> None:
        """Keep the question's input, refusing a record whose metric rows disagree on it."""
        if group_key in self._inputs and self._inputs[group_key] != input_query:
            raise ValueError(f"evaluation record {record_id!r} contains conflicting inputs")
        self._inputs[group_key] = input_query


def _expected_metrics(compiled: CompiledEval) -> dict[str, str]:
    """Map each metric the eval scores, by casefolded name, to the METRIC_TYPE its rows carry."""
    expected = {
        str(metric.name).casefold(): "system"
        for metric in compiled.resolved.config.system_metrics
        if metric.name is not None
    }
    expected.update({metric.name.casefold(): "custom" for metric in compiled.resolved.custom_metrics if metric.enabled})
    return expected


def _rows_by_name(
    columns: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
    required: tuple[str, ...],
    subject: str,
) -> tuple[dict[str, object], ...]:
    """Key each row's values by uppercased column name, once every required column is present.

    Raises:
        ValueError: a required column is missing.
    """
    names = tuple(name.upper() for name in columns)
    missing = tuple(name for name in required if name not in names)
    if missing:
        raise ValueError(f"{subject} omitted columns: {', '.join(missing)}")
    return tuple({name: row[index] for index, name in enumerate(names)} for row in rows)


def _single_row(
    columns: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
    required: tuple[str, ...],
    subject: str,
) -> Mapping[str, object]:
    """Return the one row a query must return, keyed by column name.

    Raises:
        ValueError: a required column is missing.
        SnowflakePortError: the query returned no row, or more than one.
    """
    records = _rows_by_name(columns, rows, required, subject)
    if len(records) != 1:
        raise SnowflakePortError(f"{subject} returned {len(records)} rows, expected 1")
    return records[0]


def _required_text(row: Mapping[str, object], name: str) -> str:
    """Return a column's value as text, refusing one that is null or blank."""
    value = row.get(name)
    if value is None or not str(value).strip():
        raise ValueError(f"evaluation row omitted {name}")
    return str(value)


def _result_question_key(
    row: Mapping[str, object],
    expected_questions: Mapping[str, tuple[str, Mapping[str, object]]],
) -> str:
    """Return the key of the dataset question a result row answers, found by its input.

    Snowflake returns the ground truth either whole, which must match the question's, or
    flattened to one of its fields, which must equal that field's value, compared both as the
    text Snowflake sent and as that text parsed. A question with no ground truth, as a run of
    reference-free metrics may have, matches only a row that returns none. Anything else fails.

    Raises:
        ValueError: the row has no input, an input the dataset lacks, or different ground truth.
    """
    input_query = str(row.get("INPUT") or "")
    if not input_query:
        raise ValueError("evaluation row omitted INPUT")
    expected = expected_questions.get(input_query)
    if expected is None:
        raise ValueError(f"evaluation results returned unexpected input {input_query!r}")
    expected_key, expected_ground_truth = expected
    raw = row.get("GROUND_TRUTH")
    ground_truth = _variant(raw)
    if isinstance(ground_truth, dict):
        if _question_identity(input_query, ground_truth) != expected_key:
            raise ValueError(f"evaluation input {input_query!r} returned different ground truth")
        return expected_key
    if raw is None or raw == "":
        if expected_ground_truth:
            raise ValueError(f"evaluation input {input_query!r} returned no ground truth")
        return expected_key
    projections = tuple(
        expected_ground_truth[key]
        for key in ("ground_truth_output", "ground_truth_invocations", "required_filters")
        if key in expected_ground_truth
    )
    if not any(value == projection for value in (raw, ground_truth) for projection in projections):
        raise ValueError(f"evaluation input {input_query!r} returned unrecognized flattened ground truth")
    return expected_key


def _expected_question_map(compiled: CompiledEval) -> dict[str, tuple[str, Mapping[str, object]]]:
    """Map each question of the compiled dataset, by its input, to its key and ground truth.

    Raises:
        ValueError: the compiled payload is not an array of questions, each with an input and
            ground truth, and one per input.
    """
    payload = json.loads(compiled.rendered.dataset_payload)
    if not isinstance(payload, list):
        raise ValueError("compiled evaluation dataset payload must be an array")
    values: dict[str, tuple[str, Mapping[str, object]]] = {}
    for row in payload:
        if not isinstance(row, dict) or not isinstance(row.get("input_query"), str):
            raise ValueError("compiled evaluation dataset contains an invalid question row")
        input_query = row["input_query"]
        ground_truth = row.get("ground_truth")
        if not isinstance(ground_truth, dict):
            raise ValueError("compiled evaluation dataset contains invalid ground truth")
        if input_query in values:
            raise ValueError("compiled evaluation dataset contains duplicate question inputs")
        values[input_query] = (_question_identity(input_query, ground_truth), ground_truth)
    return values


def _question_identity(input_query: str, ground_truth: Mapping[str, object]) -> str:
    """Key a question by its input and ground truth, so the key is stable across runs."""
    payload = json.dumps(
        {"ground_truth": ground_truth, "input_query": input_query},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _metric_passed(
    compiled: CompiledEval,
    metric_name: str,
    score: float | None,
    raw_status: object,
    error: object,
) -> bool:
    """Decide whether one metric passed on one question, failing closed.

    An error, a failing status, or a missing score fails the metric; otherwise the score must
    fall within the metric's threshold, when it has one.
    """
    if error is not None and str(error).strip():
        return False
    if _status_failed(raw_status):
        return False
    threshold = _metric_threshold(compiled, metric_name)
    if score is None:
        return False
    if threshold is None:
        return True
    return (threshold.min is None or score >= threshold.min) and (threshold.max is None or score <= threshold.max)


def _status_failed(raw_status: object) -> bool:
    """Report whether a metric's status code is an HTTP failure; a code that is not a number fails."""
    status = _variant(raw_status)
    status_code = status.get("status") if isinstance(status, dict) else None
    if status_code is None:
        return False
    try:
        return int(status_code) >= 400
    except (TypeError, ValueError):
        return True


def _metric_threshold(compiled: CompiledEval, metric_name: str) -> ThresholdRange | None:
    """Return a metric's pass band, by casefolded name: a system metric's, else a custom default."""
    for metric in compiled.resolved.config.system_metrics:
        if (metric.name or "").casefold() == metric_name.casefold():
            return metric.threshold
    for custom in compiled.resolved.custom_metrics:
        if custom.name.casefold() == metric_name.casefold():
            return custom.threshold_default
    return None


def _row_cost(row: Mapping[str, object]) -> EvalCostSummary:
    """Read one record's cost; the metric calls' token metadata that is not an object is skipped."""
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    calls = _variant(row.get("METRIC_CALLS"))
    if isinstance(calls, list):
        for call in calls:
            metadata = call.get("full_metadata") if isinstance(call, dict) else None
            metadata = _variant(metadata)
            if not isinstance(metadata, dict):
                continue
            prompt_tokens += _nonnegative_int(metadata.get("prompt_tokens"))
            completion_tokens += _nonnegative_int(metadata.get("completion_tokens"))
            total_tokens += _nonnegative_int(metadata.get("total_tokens"))
    return EvalCostSummary(
        duration_ms=_nonnegative_int(row.get("DURATION_MS")),
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        total_input_tokens=_nonnegative_int(row.get("TOTAL_INPUT_TOKENS")),
        total_output_tokens=_nonnegative_int(row.get("TOTAL_OUTPUT_TOKENS")),
        llm_call_count=_nonnegative_int(row.get("LLM_CALL_COUNT")),
    )


def _sum_costs(values: tuple[EvalCostSummary, ...]) -> EvalCostSummary:
    """Add up costs field by field."""
    return EvalCostSummary(
        duration_ms=sum(value.duration_ms for value in values),
        prompt_tokens=sum(value.prompt_tokens for value in values),
        completion_tokens=sum(value.completion_tokens for value in values),
        total_tokens=sum(value.total_tokens for value in values),
        total_input_tokens=sum(value.total_input_tokens for value in values),
        total_output_tokens=sum(value.total_output_tokens for value in values),
        llm_call_count=sum(value.llm_call_count for value in values),
    )


def _merge_record_cost(previous: EvalCostSummary | None, current: EvalCostSummary) -> EvalCostSummary:
    """Merge two metric rows of one request: the metric calls' tokens add, the request's own do not."""
    if previous is None:
        return current
    return EvalCostSummary(
        duration_ms=max(previous.duration_ms, current.duration_ms),
        prompt_tokens=previous.prompt_tokens + current.prompt_tokens,
        completion_tokens=previous.completion_tokens + current.completion_tokens,
        total_tokens=previous.total_tokens + current.total_tokens,
        total_input_tokens=max(previous.total_input_tokens, current.total_input_tokens),
        total_output_tokens=max(previous.total_output_tokens, current.total_output_tokens),
        llm_call_count=max(previous.llm_call_count, current.llm_call_count),
    )


def _variant(value: object) -> object:
    """Parse a VARIANT column that arrives as JSON text; any other value, or invalid JSON, as is."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _nonnegative_int(value: object) -> int:
    """Read a cost value as a non-negative integer; null counts as 0.

    Raises:
        ValueError: the value is a boolean, not a number or numeric text, or negative.
    """
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"evaluation cost value {value!r} is not an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"evaluation cost value {value!r} is not an integer") from exc
    if parsed < 0:
        raise ValueError(f"evaluation cost value {value!r} is negative")
    return parsed


def _optional_float(value: object) -> float | None:
    """Read a score as a finite number; null stays None.

    Raises:
        ValueError: the value is a boolean, not a number or numeric text, or not finite.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"evaluation score {value!r} is not numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"evaluation score {value!r} is not numeric") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"evaluation score {value!r} is not finite")
    return parsed


def _status_details(value: object) -> tuple[str, ...]:
    """Read a run's STATUS_DETAILS array as text; null reads as none, and text as one detail.

    A failed run has reported its details as bare text, such as `Invocation failed`.

    Raises:
        ValueError: the value is neither an array nor text.
    """
    parsed = _variant(value)
    if parsed is None:
        return ()
    if isinstance(parsed, str):
        return (parsed,) if parsed.strip() else ()
    if not isinstance(parsed, list):
        raise ValueError("evaluation STATUS_DETAILS must be an array")
    return tuple(str(item) for item in parsed)
