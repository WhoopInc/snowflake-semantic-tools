"""The Snowflake eval state store: metadata-only baselines and gate state in one table."""

from __future__ import annotations

import json
from typing import Mapping

from snowflake_semantic_tools.domain.model.eval import (
    EvalBaselineMetric,
    EvalBaselineRecord,
    EvalGateState,
    EvalRegression,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.sql import string_literal
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort, SnowflakePortError


class SnowflakeEvalStateStore:
    """Eval baselines and gate states in one Snowflake table: one row per target, eval and kind.

    A row is keyed by `TARGET_NAME`, `EVAL_KEY` and `RECORD_KIND` (`baseline` or `gate`) and
    holds its record as a JSON `PAYLOAD` of metadata only. A read never creates the table, so a
    read-only role can read and a missing table reads as no record; a write creates the table
    first and upserts its rows. Snowflake does not enforce the key, so a read that finds two
    rows raises.
    """

    def __init__(self, port: SnowflakePort, table: QualifiedName) -> None:
        self._port = port
        self._table = table

    def read_baseline(self, target_name: str, eval_key: str) -> EvalBaselineRecord | None:
        """Return the baseline recorded for one eval on one target; None when there is none.

        Never writes: a missing table reads as None.

        Raises:
            SnowflakePortError: The lookup or the query failed, more than one row matched, or the
                stored payload does not have a baseline's shape.
            ValueError: A stored `score_range` bound is text that is not a number.
            TypeError: A stored `score_range` bound is an array or an object.
        """
        # Reads never create the table, so a read-only role can read; no table means no record.
        if not self._port.object_exists("TABLE", self._table):
            return None
        result = self._port.query(
            f"SELECT PAYLOAD FROM {self._table.sql} "
            "WHERE TARGET_NAME = %s AND EVAL_KEY = %s AND RECORD_KIND = 'baseline'",
            (target_name, eval_key),
        )
        if not result.rows:
            return None
        if len(result.rows) != 1:
            raise SnowflakePortError(f"eval baseline state returned {len(result.rows)} rows")
        return _baseline_from_payload(result.rows[0][0])

    def write_baseline(self, target_name: str, baseline: EvalBaselineRecord) -> None:
        """Insert or replace one eval's baseline on one target, creating the table when it is absent.

        Raises:
            SnowflakePortError: Creating the table or the MERGE failed.
        """
        self._write(target_name, baseline.eval_key, "baseline", _baseline_payload(baseline))

    def write_baselines(self, target_name: str, baselines: tuple[EvalBaselineRecord, ...]) -> None:
        """Insert or replace several baselines on one target in one transaction, so a failure commits none.

        An empty batch runs nothing and does not create the table.

        Raises:
            SnowflakePortError: Creating the table or the transaction failed; a ROLLBACK is sent
                first, and its own failure is not reported.
        """
        if not baselines:
            return
        self._ensure_table()
        statements = tuple(
            self._merge_sql(target_name, baseline.eval_key, "baseline", _baseline_payload(baseline))
            for baseline in baselines
        )
        result = self._port.execute_script(("BEGIN", *statements, "COMMIT"))
        if not result.ok:
            self._port.try_execute("ROLLBACK")
            raise SnowflakePortError(result.error.message if result.error else "eval baseline batch write failed")

    def read_gate(self, target_name: str, eval_key: str) -> EvalGateState | None:
        """Return the gate state recorded for one eval on one target; None when there is none.

        Never writes: a missing table reads as None.

        Raises:
            SnowflakePortError: The lookup or the query failed, more than one row matched, or the
                stored payload does not have a gate state's shape.
            ValueError: A stored `regression_count` is text that is not an integer.
        """
        if not self._port.object_exists("TABLE", self._table):
            return None
        result = self._port.query(
            f"SELECT PAYLOAD FROM {self._table.sql} "
            "WHERE TARGET_NAME = %s AND EVAL_KEY = %s AND RECORD_KIND = 'gate'",
            (target_name, eval_key),
        )
        if not result.rows:
            return None
        if len(result.rows) != 1:
            raise SnowflakePortError(f"eval gate state returned {len(result.rows)} rows")
        return _gate_from_payload(result.rows[0][0])

    def write_gate(self, target_name: str, gate: EvalGateState) -> None:
        """Insert or replace one eval's gate state on one target, creating the table when it is absent.

        Raises:
            SnowflakePortError: Creating the table or the MERGE failed.
        """
        self._write(target_name, gate.eval_key, "gate", _gate_payload(gate))

    def _ensure_table(self) -> None:
        result = self._port.execute_script(
            (
                f"CREATE TABLE IF NOT EXISTS {self._table.sql} ("
                "TARGET_NAME VARCHAR NOT NULL, EVAL_KEY VARCHAR NOT NULL, "
                "RECORD_KIND VARCHAR NOT NULL, PAYLOAD OBJECT NOT NULL, UPDATED_AT TIMESTAMP_TZ NOT NULL, "
                "PRIMARY KEY (TARGET_NAME, EVAL_KEY, RECORD_KIND))",
            )
        )
        if not result.ok:
            raise SnowflakePortError(result.error.message if result.error else "eval state table creation failed")

    def _write(self, target_name: str, eval_key: str, kind: str, payload: Mapping[str, object]) -> None:
        self._ensure_table()
        result = self._port.execute_script((self._merge_sql(target_name, eval_key, kind, payload),))
        if not result.ok:
            raise SnowflakePortError(result.error.message if result.error else "eval state write failed")

    def _merge_sql(self, target_name: str, eval_key: str, kind: str, payload: Mapping[str, object]) -> str:
        return (
            f"MERGE INTO {self._table.sql} AS target USING (SELECT "
            f"{string_literal(target_name)} TARGET_NAME, {string_literal(eval_key)} EVAL_KEY, "
            f"{string_literal(kind)} RECORD_KIND, PARSE_JSON({string_literal(json.dumps(payload, sort_keys=True, separators=(',', ':')))}) PAYLOAD, "
            "CURRENT_TIMESTAMP() UPDATED_AT) source "
            "ON target.TARGET_NAME = source.TARGET_NAME AND target.EVAL_KEY = source.EVAL_KEY "
            "AND target.RECORD_KIND = source.RECORD_KIND "
            "WHEN MATCHED THEN UPDATE SET PAYLOAD = source.PAYLOAD, UPDATED_AT = source.UPDATED_AT "
            "WHEN NOT MATCHED THEN INSERT (TARGET_NAME, EVAL_KEY, RECORD_KIND, PAYLOAD, UPDATED_AT) "
            "VALUES (source.TARGET_NAME, source.EVAL_KEY, source.RECORD_KIND, source.PAYLOAD, source.UPDATED_AT)"
        )


def _baseline_payload(value: EvalBaselineRecord) -> dict[str, object]:
    return {
        "eval_key": value.eval_key,
        "dataset_fingerprint": value.dataset_fingerprint,
        "config_fingerprint": value.config_fingerprint,
        "agent_version": value.agent_version,
        "metric_versions": dict(value.metric_versions),
        "metrics": [
            {
                "question_key": metric.question_key,
                "metric_name": metric.metric_name,
                "passed_attempts": list(metric.passed_attempts),
                "score_range": list(metric.score_range) if metric.score_range is not None else None,
            }
            for metric in value.metrics
        ],
        "run_names": list(value.run_names),
        "captured_at": value.captured_at,
        "expires_at": value.expires_at,
        "reason": value.reason,
        "tier": value.tier,
        "gate_policy": dict(value.gate_policy),
    }


def _baseline_from_payload(value: object) -> EvalBaselineRecord:
    """Decode a stored baseline payload, given as a mapping or as its JSON text.

    A missing field reads as empty: `""`, no metrics or run names, and `report` for the tier.
    Metric versions and the gate policy are sorted by name, so the record does not depend on
    the order the payload holds them in; a `score_range` bound of null stays None.

    Raises:
        SnowflakePortError: The payload, a metric, the metric versions or the gate policy is not
            an object; `metrics` or `run_names` is not an array; `passed_attempts` holds a
            non-boolean; or a `score_range` does not hold two values.
        ValueError: A `score_range` bound is text that is not a number.
        TypeError: A `score_range` bound is an array or an object.
    """
    payload = _mapping(value, "baseline")
    metrics = payload.get("metrics", [])
    run_names = payload.get("run_names", [])
    if not isinstance(metrics, list) or not isinstance(run_names, list):
        raise SnowflakePortError("eval baseline metrics must be an array")
    metric_versions = _mapping(payload.get("metric_versions", {}), "metric versions")
    gate_policy = _mapping(payload.get("gate_policy", {}), "gate policy")
    parsed_metrics = [_baseline_metric(item) for item in metrics]
    return EvalBaselineRecord(
        eval_key=str(payload.get("eval_key") or ""),
        dataset_fingerprint=str(payload.get("dataset_fingerprint") or ""),
        config_fingerprint=str(payload.get("config_fingerprint") or ""),
        agent_version=str(payload.get("agent_version") or ""),
        metric_versions=tuple(sorted((str(key), str(item)) for key, item in metric_versions.items())),
        metrics=tuple(parsed_metrics),
        run_names=tuple(str(item) for item in run_names),
        captured_at=str(payload.get("captured_at") or ""),
        expires_at=str(payload.get("expires_at") or ""),
        reason=str(payload.get("reason") or ""),
        tier=str(payload.get("tier") or "report"),
        gate_policy=tuple(sorted((str(key), bool(item)) for key, item in gate_policy.items())),
    )


def _baseline_metric(value: object) -> EvalBaselineMetric:
    """Decode one metric of a stored baseline; `_baseline_from_payload` lists what it rejects."""
    metric = _mapping(value, "baseline metric")
    passed_attempts = metric.get("passed_attempts", [])
    score_range = metric.get("score_range")
    if not isinstance(passed_attempts, list) or any(not isinstance(flag, bool) for flag in passed_attempts):
        raise SnowflakePortError("eval baseline passed_attempts must be booleans")
    if score_range is not None:
        if not isinstance(score_range, list) or len(score_range) != 2:
            raise SnowflakePortError("eval baseline score_range must have two values")
        parsed_range = tuple(float(item) if item is not None else None for item in score_range)
    else:
        parsed_range = None
    return EvalBaselineMetric(
        str(metric.get("question_key") or ""),
        str(metric.get("metric_name") or ""),
        tuple(passed_attempts),
        parsed_range,  # type: ignore[arg-type]
    )


def _gate_payload(value: EvalGateState) -> dict[str, object]:
    return {
        "eval_key": value.eval_key,
        "tier": value.tier,
        "regression_count": value.regression_count,
        "regressions": [
            {"question_key": item.question_key, "metric_name": item.metric_name} for item in value.regressions
        ],
        "unresolved": value.unresolved,
        "evaluated_at": value.evaluated_at,
        "run_names": list(value.run_names),
    }


def _gate_from_payload(value: object) -> EvalGateState:
    """Decode a stored gate payload, given as a mapping or as its JSON text.

    A missing field reads as empty: `""`, no regressions or run names, a count of 0, and not
    unresolved. `regression_count` may be stored as a number or as numeric text.

    Raises:
        SnowflakePortError: The payload or a regression is not an object, `regressions` or
            `run_names` is not an array, or `regression_count` is a boolean or not a number.
        ValueError: `regression_count` is text that is not an integer.
    """
    payload = _mapping(value, "gate")
    regressions = payload.get("regressions", [])
    run_names = payload.get("run_names", [])
    if not isinstance(regressions, list) or not isinstance(run_names, list):
        raise SnowflakePortError("eval gate regressions must be an array")
    regression_count = payload.get("regression_count", 0)
    if isinstance(regression_count, bool) or not isinstance(regression_count, (int, float, str)):
        raise SnowflakePortError("eval gate regression_count must be an integer")
    return EvalGateState(
        eval_key=str(payload.get("eval_key") or ""),
        tier=str(payload.get("tier") or ""),
        regression_count=int(regression_count),
        regressions=tuple(
            EvalRegression(
                str(_mapping(item, "regression").get("question_key") or ""),
                str(_mapping(item, "regression").get("metric_name") or ""),
            )
            for item in regressions
        ),
        unresolved=bool(payload.get("unresolved")),
        evaluated_at=str(payload.get("evaluated_at") or ""),
        run_names=tuple(str(item) for item in run_names),
    )


def _mapping(value: object, subject: str) -> dict[str, object]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise SnowflakePortError(f"eval {subject} payload must be an object")
    return {str(key): item for key, item in value.items()}
