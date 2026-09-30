"""Start, poll, retrieve, and normalize Cortex Agent evaluation runs."""

from __future__ import annotations

import json
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import md5, sha256
from typing import Mapping, Sequence

from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ..domain.model.eval import (
    EVAL_PASS_STATUSES,
    EVAL_TERMINAL_STATUSES,
    EvalCostSummary,
    EvalDefaults,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    render_eval_name_template,
)
from ..domain.model.identifier import SchemaScope
from ..domain.model.lifecycle import Action
from ..domain.ports.snowflake import ClockPort, SnowflakePort, SnowflakePortError
from ..domain.state.model import Manifest, State
from .eval_compile import CompiledEval
from .eval_lifecycle import EvalLifecycleConfig, EvalLifecycleHandler

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
_PARTIAL_STATUSES = frozenset(("INVOCATION_PARTIALLY_COMPLETED", "PARTIALLY_COMPLETED"))
_DEFAULT_RUN_NAME_TEMPLATE = "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}"


@dataclass(frozen=True, slots=True)
class EvalRunOptions:
    git_sha: str
    timestamp: str | None = None
    poll_interval_ms: int = 5_000
    max_polls: int = 240


@dataclass(frozen=True, slots=True)
class EvalRunResult:
    eval_key: str
    attempts: tuple[EvalRunAttempt, ...]
    diagnostics: DiagnosticBag
    accepted: bool = False

    @property
    def success(self) -> bool:
        return self.accepted


@dataclass(frozen=True, slots=True)
class EvalSuiteResult:
    evals: tuple[EvalRunResult, ...]
    diagnostics: DiagnosticBag

    @property
    def success(self) -> bool:
        return bool(self.evals) and not self.diagnostics.has_errors and all(result.success for result in self.evals)


class RunEvalSuite:
    def __init__(
        self,
        port: SnowflakePort,
        clock: ClockPort,
        lifecycle_config: EvalLifecycleConfig = EvalLifecycleConfig(),
    ) -> None:
        self._port = port
        self._clock = clock
        self._lifecycle_config = lifecycle_config

    def run(
        self,
        compiled: Sequence[CompiledEval],
        *,
        defaults: EvalDefaults = EvalDefaults(),
        options: EvalRunOptions,
        fail_fast: bool = False,
        config_digests: Mapping[str, str] | None = None,
        baseline_capture: bool = False,
    ) -> EvalSuiteResult:
        config_digests = config_digests or {}
        if fail_fast or len(compiled) < 2:
            results = []
            for item in compiled:
                result = self._run_eval(
                    item,
                    defaults,
                    options,
                    config_digests.get(item.artifact_key),
                    baseline_capture,
                    defaults.baseline_runs,
                )
                results.append(result)
                if fail_fast and not result.success:
                    break
        else:
            requested: list[int] = []
            for item in compiled:
                run = item.resolved.config.run
                if run is not None and run.concurrency is not None:
                    requested.append(run.concurrency)
            if defaults.concurrency:
                ceiling = max(1, defaults.concurrency)
            else:
                ceiling = max(1, max(requested, default=1))
            with ThreadPoolExecutor(max_workers=ceiling) as pool:
                results = list(
                    pool.map(
                        lambda item: self._run_eval(
                            item,
                            defaults,
                            options,
                            config_digests.get(item.artifact_key),
                            baseline_capture,
                            defaults.baseline_runs,
                        ),
                        compiled,
                    )
                )
        diagnostics = [diagnostic for result in results for diagnostic in result.diagnostics]
        return EvalSuiteResult(tuple(results), DiagnosticBag(tuple(diagnostics)))

    def _run_eval(
        self,
        compiled: CompiledEval,
        defaults: EvalDefaults,
        options: EvalRunOptions,
        config_digest: str | None,
        baseline_capture: bool,
        default_baseline_runs: int | None,
    ) -> EvalRunResult:
        run = compiled.resolved.config.run
        if run is None:
            diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail="run configuration is absent")
            return EvalRunResult(compiled.artifact_key, (), DiagnosticBag((diagnostic,)))
        timestamp = options.timestamp or _compact_timestamp()
        retry_count = run.retry if run.retry is not None else defaults.retry or 0
        try:
            resolved_agent_version = self._port.resolve_agent_version(
                compiled.agent_target,
                compiled.resolved.config.agent_version or "",
            )
        except SnowflakePortError as exc:
            diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
            return EvalRunResult(compiled.artifact_key, (), DiagnosticBag((diagnostic,)))
        required_completed = (run.baseline_runs or default_baseline_runs or 1) if baseline_capture else 1
        attempt_limit = required_completed + retry_count if baseline_capture else retry_count + 1
        config_path = EvalLifecycleHandler(self._port, self._lifecycle_config).config_path(compiled.rendered_artifact)
        try:
            self._ensure_config(config_path, compiled.rendered.config_yaml.encode("utf-8"), config_digest)
        except SnowflakePortError as exc:
            diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
            return EvalRunResult(compiled.artifact_key, (), DiagnosticBag((diagnostic,)))
        attempts: list[EvalRunAttempt] = []
        diagnostics: list[Diagnostic] = []
        accepted = False
        completed_count = 0
        for attempt_number in range(1, attempt_limit + 1):
            base_name = render_eval_name_template(
                run.name_template or _DEFAULT_RUN_NAME_TEMPLATE,
                agent=compiled.name,
                sha7=options.git_sha[:7],
                variant=run.variant or "ci",
                ts=timestamp,
            )
            run_name = base_name if attempt_number == 1 else f"{base_name}_R{attempt_number}"
            started = self._start(compiled, run_name, config_path)
            if started is not None:
                diagnostics.append(started)
                attempts.append(
                    EvalRunAttempt(run_name, attempt_number, "START_FAILED", retrieval_error=started.message)
                )
                continue
            try:
                terminal_status, status_details = self._poll(compiled, run_name, config_path, options)
            except (SnowflakePortError, ValueError) as exc:
                diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
                diagnostics.append(diagnostic)
                attempts.append(EvalRunAttempt(run_name, attempt_number, "STATUS_FAILED", retrieval_error=str(exc)))
                continue
            if terminal_status in _PARTIAL_STATUSES:
                diagnostics.append(D("SST-APL024", artifact=compiled.artifact_key, found=terminal_status))
            if terminal_status not in EVAL_PASS_STATUSES:
                attempts.append(
                    EvalRunAttempt(
                        run_name,
                        attempt_number,
                        terminal_status,
                        status_details=status_details,
                    )
                )
                if terminal_status == "CANCELLED":
                    break
                continue
            try:
                rows, cost = self._retrieve(compiled, run_name)
                attempts.append(
                    EvalRunAttempt(
                        run_name,
                        attempt_number,
                        terminal_status,
                        rows,
                        cost,
                        status_details=status_details,
                        agent_version=resolved_agent_version,
                    )
                )
                accepted = True
                completed_count += 1
                if baseline_capture and completed_count >= required_completed:
                    break
            except (SnowflakePortError, ValueError) as exc:
                attempts.append(
                    EvalRunAttempt(
                        run_name,
                        attempt_number,
                        terminal_status,
                        retrieval_error=str(exc),
                        status_details=status_details,
                        agent_version=resolved_agent_version,
                    )
                )
                diagnostics.append(D("SST-SNO001", detail=f"eval '{compiled.artifact_key}' retrieval failed: {exc}"))
        return EvalRunResult(compiled.artifact_key, tuple(attempts), DiagnosticBag(tuple(diagnostics)), accepted)

    def _ensure_config(self, config_path: str, content: bytes, trusted_digest: str | None) -> None:
        observed = self._port.observe_staged_file(config_path)
        staged_content = self._port.read_staged_file(config_path) if observed is not None else None
        staged_digest = md5(staged_content, usedforsecurity=False).hexdigest() if staged_content is not None else None
        if observed is None or staged_content != content or trusted_digest is None or staged_digest != trusted_digest:
            self._port.upload(config_path, content)
            observed = self._port.observe_staged_file(config_path)
            staged_content = self._port.read_staged_file(config_path) if observed is not None else None
            staged_digest = (
                md5(staged_content, usedforsecurity=False).hexdigest() if staged_content is not None else None
            )
        if observed is None:
            raise SnowflakePortError(f"staged evaluation config {config_path} is absent")
        if staged_content is None:
            raise SnowflakePortError(f"staged evaluation config {config_path} is unreadable")
        if len(staged_content) != len(content):
            raise SnowflakePortError(
                f"staged evaluation config {config_path} has {len(staged_content)} bytes, expected {len(content)}"
            )
        if trusted_digest is not None and staged_digest != trusted_digest:
            raise SnowflakePortError(
                f"staged evaluation config {config_path} has digest {staged_digest}, expected {trusted_digest}"
            )
        if staged_content != content:
            raise SnowflakePortError(f"staged evaluation config {config_path} bytes do not match rendered config")

    def _start(self, compiled: CompiledEval, run_name: str, config_path: str) -> Diagnostic | None:
        result = self._port.execute_script(
            (
                f"USE DATABASE {compiled.agent_target.database.sql}",
                f"USE SCHEMA {compiled.agent_target.database.sql}.{compiled.agent_target.schema.sql}",
                _evaluation_call("START", run_name, config_path),
            )
        )
        if not result.ok:
            detail = result.error.message if result.error is not None else "EXECUTE_AI_EVALUATION returned no result"
            return D("SST-APL023", artifact=compiled.artifact_key, detail=detail)
        return None

    def _poll(
        self,
        compiled: CompiledEval,
        run_name: str,
        config_path: str,
        options: EvalRunOptions,
    ) -> tuple[str, tuple[str, ...]]:
        for poll in range(options.max_polls):
            result = self._port.query_in_context(
                SchemaScope(compiled.agent_target.database, compiled.agent_target.schema),
                _evaluation_status_call(),
                (run_name, config_path),
            )
            row = _single_row(result.columns, result.rows, _STATUS_COLUMNS, "evaluation status")
            if _required_text(row, "RUN_NAME") != run_name:
                raise SnowflakePortError(f"evaluation status returned run {_required_text(row, 'RUN_NAME')!r}")
            if _required_text(row, "AGENT_NAME").casefold() != compiled.agent_target.artifact_name:
                raise SnowflakePortError("evaluation status returned a different agent")
            if _required_text(row, "AGENT_TYPE").upper() != "CORTEX AGENT":
                raise SnowflakePortError("evaluation status returned a different agent type")
            status = _required_text(row, "STATUS").upper()
            details = _status_details(row.get("STATUS_DETAILS"))
            if status in EVAL_TERMINAL_STATUSES:
                return status, details
            if poll + 1 < options.max_polls:
                self._clock.sleep(options.poll_interval_ms)
        raise SnowflakePortError(f"evaluation run {run_name!r} did not reach a terminal status")

    def _retrieve(self, compiled: CompiledEval, run_name: str) -> tuple[tuple[EvalResultRow, ...], EvalCostSummary]:
        result = self._port.query_in_context(
            SchemaScope(compiled.agent_target.database, compiled.agent_target.schema),
            "SELECT * FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(%s, %s, %s, %s, %s))",
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
        grouped: dict[tuple[str, str], list[EvalMetricResult]] = {}
        costs: dict[str, EvalCostSummary] = {}
        inputs: dict[tuple[str, str], str] = {}
        seen_metrics: set[tuple[str, str, str]] = set()
        record_questions: dict[str, str] = {}
        question_records: dict[str, str] = {}
        expected_metrics = {
            str(metric.name).casefold(): "system"
            for metric in compiled.resolved.config.system_metrics
            if metric.name is not None
        }
        expected_metrics.update(
            {metric.name.casefold(): "custom" for metric in compiled.resolved.custom_metrics if metric.enabled}
        )
        expected_questions = _expected_question_map(compiled)
        for record in records:
            question_key = _result_question_key(record, expected_questions)
            record_id = _required_text(record, "RECORD_ID")
            if record_id in record_questions and record_questions[record_id] != question_key:
                raise ValueError(f"evaluation record {record_id!r} maps to multiple questions")
            if question_key in question_records and question_records[question_key] != record_id:
                raise ValueError(f"evaluation question {question_key!r} maps to multiple records")
            record_questions[record_id] = question_key
            question_records[question_key] = record_id
            group_key = (record_id, question_key)
            metric_name = _required_text(record, "METRIC_NAME")
            metric_type = _required_text(record, "METRIC_TYPE").casefold()
            expected_type = expected_metrics.get(metric_name.casefold())
            if expected_type is None:
                raise ValueError(f"evaluation results returned unknown metric {metric_name!r}")
            if metric_type != expected_type:
                raise ValueError(
                    f"evaluation metric {metric_name!r} has type {metric_type!r}, expected {expected_type!r}"
                )
            duplicate_key = (record_id, metric_name.casefold(), metric_type)
            if duplicate_key in seen_metrics:
                raise ValueError(f"evaluation results contain duplicate metric row {duplicate_key!r}")
            seen_metrics.add(duplicate_key)
            score = _optional_float(record.get("EVAL_AGG_SCORE"))
            passed = _metric_passed(
                compiled,
                metric_name,
                score,
                record.get("METRIC_STATUS"),
                record.get("ERROR"),
            )
            grouped.setdefault(group_key, []).append(EvalMetricResult(question_key, metric_name, score, passed))
            input_query = str(record.get("INPUT") or "")
            if group_key in inputs and inputs[group_key] != input_query:
                raise ValueError(f"evaluation record {record_id!r} contains conflicting inputs")
            inputs[group_key] = input_query
            request_id = _required_text(record, "REQUEST_ID")
            costs[request_id] = _merge_record_cost(costs.get(request_id), _row_cost(record))
        expected_question_keys = frozenset(question_key for question_key, _ in expected_questions.values())
        found_questions = frozenset(question_records)
        if found_questions != expected_question_keys:
            raise ValueError(
                "evaluation results question set differs from the authored dataset: "
                f"missing={sorted(expected_question_keys - found_questions)!r}, "
                f"unexpected={sorted(found_questions - expected_question_keys)!r}"
            )
        expected_metric_names = frozenset(expected_metrics)
        for (_, question_key), metrics in grouped.items():
            found_metric_names = frozenset(metric.metric_name.casefold() for metric in metrics)
            if found_metric_names != expected_metric_names:
                raise ValueError(
                    f"evaluation question {question_key!r} metric set differs from config: "
                    f"missing={sorted(expected_metric_names - found_metric_names)!r}, "
                    f"unexpected={sorted(found_metric_names - expected_metric_names)!r}"
                )
        rows = tuple(
            EvalResultRow(question_key, inputs[group_key], tuple(sorted(metrics, key=lambda item: item.metric_name)))
            for group_key, metrics in sorted(grouped.items(), key=lambda item: item[0])
            for question_key in (group_key[1],)
        )
        return rows, _sum_costs(tuple(costs.values()))


def validate_eval_publication(
    compiled: Sequence[CompiledEval],
    manifest: Manifest,
    state: State,
    handler: EvalLifecycleHandler,
) -> DiagnosticBag:
    diagnostics: list[Diagnostic] = []
    for item in compiled:
        artifact = item.rendered_for_publish(manifest.manifest_id)
        entry = state.applied.get(item.artifact_key)
        if (
            entry is None
            or entry.fingerprint != artifact.fingerprint
            or entry.manifest_id != manifest.manifest_id
            or entry.qualified_name.casefold() != artifact.target.sql.casefold()
            or entry.outcome != "applied"
        ):
            diagnostics.append(D("SST-APL012", artifact=item.artifact_key, value=artifact.target.sql))
            continue
        plan = handler.plan(artifact, entry, manifest)
        diagnostics.extend(plan.diagnostics)
        if plan.action is not Action.NOOP and not plan.diagnostics.has_errors:
            diagnostics.append(D("SST-APL012", artifact=item.artifact_key, value=artifact.target.sql))
    return DiagnosticBag(tuple(diagnostics))


def _evaluation_call(job: str, run_name: str, config_path: str) -> str:
    return (
        f"CALL EXECUTE_AI_EVALUATION('{job}', "
        f"OBJECT_CONSTRUCT('run_name', {_sql_literal(run_name)}), {_sql_literal(config_path)})"
    )


def _evaluation_status_call() -> str:
    return "CALL EXECUTE_AI_EVALUATION('STATUS', OBJECT_CONSTRUCT('run_name', %s), %s)"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _rows_by_name(
    columns: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
    required: tuple[str, ...],
    subject: str,
) -> tuple[dict[str, object], ...]:
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
    records = _rows_by_name(columns, rows, required, subject)
    if len(records) != 1:
        raise SnowflakePortError(f"{subject} returned {len(records)} rows, expected 1")
    return records[0]


def _required_text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if value is None or not str(value).strip():
        raise ValueError(f"evaluation row omitted {name}")
    return str(value)


def _result_question_key(
    row: Mapping[str, object],
    expected_questions: Mapping[str, tuple[str, Mapping[str, object]]],
) -> str:
    input_query = str(row.get("INPUT") or "")
    if not input_query:
        raise ValueError("evaluation row omitted INPUT")
    expected = expected_questions.get(input_query)
    if expected is None:
        raise ValueError(f"evaluation results returned unexpected input {input_query!r}")
    expected_key, expected_ground_truth = expected
    ground_truth = _variant(row.get("GROUND_TRUTH"))
    if isinstance(ground_truth, dict):
        if _question_identity(input_query, ground_truth) != expected_key:
            raise ValueError(f"evaluation input {input_query!r} returned different ground truth")
        return expected_key
    projections = tuple(
        expected_ground_truth[key]
        for key in ("ground_truth_output", "ground_truth_invocations", "required_filters")
        if key in expected_ground_truth
    )
    if ground_truth not in projections:
        raise ValueError(f"evaluation input {input_query!r} returned unrecognized flattened ground truth")
    return expected_key


def _expected_question_map(compiled: CompiledEval) -> dict[str, tuple[str, Mapping[str, object]]]:
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
    if error is not None and str(error).strip():
        return False
    status = _variant(raw_status)
    status_code = status.get("status") if isinstance(status, dict) else None
    if status_code is not None:
        try:
            if int(status_code) >= 400:
                return False
        except (TypeError, ValueError):
            return False
    metric = next(
        (
            item
            for item in compiled.resolved.config.system_metrics
            if (item.name or "").casefold() == metric_name.casefold()
        ),
        None,
    )
    threshold = metric.threshold if metric is not None else None
    if metric is None:
        custom = next(
            (item for item in compiled.resolved.custom_metrics if item.name.casefold() == metric_name.casefold()),
            None,
        )
        threshold = custom.threshold_default if custom is not None else None
    if score is None:
        return False
    if threshold is None:
        return True
    return (threshold.min is None or score >= threshold.min) and (threshold.max is None or score <= threshold.max)


def _row_cost(row: Mapping[str, object]) -> EvalCostSummary:
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
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _nonnegative_int(value: object) -> int:
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
    parsed = _variant(value)
    if parsed is None:
        return ()
    if not isinstance(parsed, list):
        raise ValueError("evaluation STATUS_DETAILS must be an array")
    return tuple(str(item) for item in parsed)


def _compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def eval_suite_json(result: EvalSuiteResult) -> dict[str, object]:
    attempts = tuple(attempt for eval_result in result.evals for attempt in eval_result.attempts)
    total_cost = _sum_costs(tuple(attempt.cost for attempt in attempts))
    return {
        "evals": [
            {
                "eval_key": eval_result.eval_key,
                "attempts": [
                    {
                        "run_name": attempt.run_name,
                        "attempt": attempt.attempt,
                        "terminal_status": attempt.terminal_status,
                        "status_details": list(attempt.status_details),
                        "retrieval_error": attempt.retrieval_error,
                        "metric_summaries": _metric_summaries(attempt.rows),
                        "cost": _cost_json(attempt.cost),
                    }
                    for attempt in eval_result.attempts
                ],
            }
            for eval_result in result.evals
        ],
        "attempt_count": len(attempts),
        "cost_totals": _cost_json(total_cost),
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def empty_eval_suite_json() -> dict[str, object]:
    return {
        "evals": [],
        "attempt_count": 0,
        "cost_totals": _cost_json(EvalCostSummary()),
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def _metric_summaries(rows: tuple[EvalResultRow, ...]) -> list[dict[str, object]]:
    values: dict[str, list[EvalMetricResult]] = {}
    for row in rows:
        for metric in row.metrics:
            values.setdefault(metric.metric_name, []).append(metric)
    return [
        {
            "metric_name": name,
            "record_count": len(metrics),
            "passed_count": sum(metric.passed for metric in metrics),
            "average_score": (
                sum(score for metric in metrics if (score := metric.score) is not None)
                / sum(metric.score is not None for metric in metrics)
                if any(metric.score is not None for metric in metrics)
                else None
            ),
        }
        for name, metrics in sorted(values.items())
    ]


def _cost_json(value: EvalCostSummary) -> dict[str, object]:
    return {
        "duration_ms": value.duration_ms,
        "prompt_tokens": value.prompt_tokens,
        "completion_tokens": value.completion_tokens,
        "total_tokens": value.total_tokens,
        "total_input_tokens": value.total_input_tokens,
        "total_output_tokens": value.total_output_tokens,
        "llm_call_count": value.llm_call_count,
        "credits": None,
        "credit_attribution": "not_requested",
    }
