"""The usage views a comparison's cost is read from: agent requests, and the judges of each run.

Both reads bind every value: the request ids and run names as one JSON array, which
`FLATTEN(PARSE_JSON(...))` expands, so no id reaches the statement's text and a run of any
size is one statement. Columns are read by
position, in the order each SELECT lists them.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage, JudgeUsage, UsagePort
from snowflake_semantic_tools.domain.sql import Sql, sql


# MEASURED: an evaluation's agent request has one CORTEX_AGENT_USAGE_HISTORY row, keyed by the
# REQUEST_ID its results report; METADATA's sql_query_credits is often null.
def agent_usage_sql() -> Sql:
    """Return the read of the agent usage rows of some requests, started since a bound time.

    Binds: the earliest start time, then the request ids as a JSON array.
    """
    return sql(
        "SELECT REQUEST_ID, TOKEN_CREDITS, METADATA:sql_query_credits::FLOAT AS SQL_QUERY_CREDITS, START_TIME\n"
        "FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY\n"
        "WHERE START_TIME >= %s::TIMESTAMP_TZ\n"
        "  AND REQUEST_ID IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(INPUT => PARSE_JSON(%s))))"
    )


# MEASURED: an eval span lists its judge requests in inference_request_ids as one comma-separated
# VARCHAR, and each joins CORTEX_REST_API_USAGE_HISTORY.REQUEST_ID, which has tokens, no credits.
def judge_usage_sql() -> Sql:
    """Return the read of each named run's judge requests, and the tokens of those that landed.

    An eval span lists its judge requests as one comma-separated VARCHAR, so each is split out
    and trimmed, and each request counts once per run however many spans name it. Binds: the run
    names as a JSON array.
    """
    return sql(
        "WITH SPANS AS (\n"
        "    SELECT DISTINCT\n"
        '        EVENTS.RECORD_ATTRIBUTES:"snow.ai.observability.run.name"::VARCHAR AS RUN_NAME\n'
        "      , TRIM(IDS.VALUE::VARCHAR) AS REQUEST_ID\n"
        "    FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS EVENTS\n"
        "      , LATERAL SPLIT_TO_TABLE(\n"
        "            EVENTS.RECORD_ATTRIBUTES:\"ai.observability.eval.inference_request_ids\"::VARCHAR, ','\n"
        "        ) IDS\n"
        "    WHERE EVENTS.RECORD_ATTRIBUTES:\"ai.observability.span_type\"::VARCHAR = 'eval'\n"
        '      AND EVENTS.RECORD_ATTRIBUTES:"snow.ai.observability.run.name"::VARCHAR\n'
        "        IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(INPUT => PARSE_JSON(%s))))\n"
        "      AND TRIM(IDS.VALUE::VARCHAR) <> ''\n"
        ")\n"
        "SELECT SPANS.RUN_NAME\n"
        "  , COUNT(DISTINCT SPANS.REQUEST_ID) AS REQUESTS\n"
        "  , COUNT(DISTINCT REST_USAGE.REQUEST_ID) AS LANDED\n"
        "  , COALESCE(SUM(REST_USAGE.TOKENS), 0) AS TOKENS\n"
        "FROM SPANS\n"
        "LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_REST_API_USAGE_HISTORY REST_USAGE\n"
        "  ON REST_USAGE.REQUEST_ID = SPANS.REQUEST_ID\n"
        "GROUP BY SPANS.RUN_NAME"
    )


def agent_usage_row(row: Sequence[object]) -> AgentUsage:
    """Read one agent usage row, by position.

    Raises:
        SnowflakePortError: the row has no request id, or a credit that is not a number.
    """
    request_id, credits, sql_credits, started = (*row, None, None, None, None)[:4]
    if not request_id:
        raise SnowflakePortError("an agent usage row has no REQUEST_ID")
    return AgentUsage(
        str(request_id),
        _number(credits, "TOKEN_CREDITS") or 0.0,
        _number(sql_credits, "sql_query_credits"),
        started.isoformat() if isinstance(started, datetime) else str(started or ""),
    )


def judge_usage_row(row: Sequence[object]) -> JudgeUsage:
    """Read one run's judge usage row, by position.

    Raises:
        SnowflakePortError: the row has no run name, or a count that is not a number.
    """
    run_name, requests, landed, tokens = (*row, None, None, None, None)[:4]
    if not run_name:
        raise SnowflakePortError("a judge usage row has no RUN_NAME")
    return JudgeUsage(
        str(run_name),
        int(_number(requests, "REQUESTS") or 0),
        int(_number(landed, "LANDED") or 0),
        int(_number(tokens, "TOKENS") or 0),
    )


def _number(value: object, column: str) -> float | None:
    """Read a numeric cell, which the driver may return as a Decimal or text; None for NULL."""
    if value is None:
        return None
    try:
        return float(str(value))
    except ValueError as exc:
        raise SnowflakePortError(f"{column} is {value!r}, not a number") from exc


class UsageMethods(Session, UsagePort):
    """Read a comparison's cost from the account's usage views."""

    def agent_usage(self, request_ids: Sequence[str], *, since: str) -> tuple[AgentUsage, ...]:
        if not request_ids:
            return ()
        result = self.query(agent_usage_sql(), (since, json.dumps(sorted(request_ids))))
        return tuple(agent_usage_row(row) for row in result.rows)

    def judge_usage(self, run_names: Sequence[str]) -> tuple[JudgeUsage, ...]:
        if not run_names:
            return ()
        result = self.query(judge_usage_sql(), (json.dumps(sorted(run_names)),))
        return tuple(judge_usage_row(row) for row in result.rows)
