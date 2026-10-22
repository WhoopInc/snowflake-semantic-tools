"""Read what each model's runs of a comparison cost, from the account's usage views.

One read of agent usage covers every request of every model, and one read of judge usage every
run, so a comparison reads its cost in two statements however many models it ran. Each amount is
`known`, `pending` (a request or a judge call has no usage row yet: the views lag up to an hour,
and an agent row's SQL credits up to eight), or `unavailable` (the read was refused, or nothing
names the requests to read). A CoWork profile's agent usage is its router's alone: the subagent
runs it routed to are requests of their own, which nothing ties back to the run.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from snowflake_semantic_tools.domain.evaluate.compare import KNOWN, PENDING, UNAVAILABLE, Amount, ModelCost
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage, JudgeUsage, UsagePort

# How long after its request a row's SQL credits may still be null, and land later.
SQL_CREDIT_LAG = timedelta(hours=8)
# How far before the comparison started the agent usage read looks, allowing for clock skew.
CLOCK_SKEW = timedelta(hours=1)


@dataclass(frozen=True, slots=True)
class CostQuery:
    """What one model's cost is read by: the agent requests its runs made, and the runs' names."""

    eval_key: str
    model: str
    request_ids: tuple[str, ...]
    run_names: tuple[str, ...]


def read_costs(
    port: UsagePort,
    queries: Sequence[CostQuery],
    *,
    started_at: str,
    now: str,
    lookback: timedelta = CLOCK_SKEW,
) -> Mapping[tuple[str, str], ModelCost]:
    """Read every model's cost by `(eval key, model)`; a refused read is unavailable, never raised.

    Args:
        started_at: When the comparison started; the agent read looks from `lookback` before it.
        now: When the cost is read; a null SQL credit on a request younger than
            `SQL_CREDIT_LAG` is pending, since it may still land.
    """
    start = _moment(started_at)
    since = (start - lookback).isoformat() if start is not None else started_at
    request_ids = sorted({item for query in queries for item in query.request_ids})
    run_names = sorted({item for query in queries for item in query.run_names})
    agent = _agent_rows(port, request_ids, since)
    judges = _judge_rows(port, run_names)
    moment = _moment(now)
    return {(query.eval_key, query.model): _cost(query, agent, judges, moment) for query in queries}


def _agent_rows(port: UsagePort, request_ids: Sequence[str], since: str) -> Mapping[str, AgentUsage] | str:
    """Read the agent usage of every request, by request id; the refusal's first line when refused."""
    try:
        return {row.request_id: row for row in port.agent_usage(request_ids, since=since)}
    except SnowflakePortError as exc:
        return _first_line(exc)


def _judge_rows(port: UsagePort, run_names: Sequence[str]) -> Mapping[str, JudgeUsage] | str:
    """Read the judges' usage of every run, by run name; the refusal's first line when refused."""
    try:
        return {row.run_name: row for row in port.judge_usage(run_names)}
    except SnowflakePortError as exc:
        return _first_line(exc)


def _cost(
    query: CostQuery,
    agent: Mapping[str, AgentUsage] | str,
    judges: Mapping[str, JudgeUsage] | str,
    now: datetime | None,
) -> ModelCost:
    credits, sql = _agent(query, agent, now)
    return ModelCost(credits, sql, _judge(query, judges))


def _agent(query: CostQuery, rows: Mapping[str, AgentUsage] | str, now: datetime | None) -> tuple[Amount, Amount]:
    """Price a model's agent requests, then their SQL, as the module docstring says."""
    if isinstance(rows, str):
        return Amount(UNAVAILABLE, reason=rows), Amount(UNAVAILABLE, reason=rows)
    if not query.request_ids:
        missing = "its runs' results named no agent request"
        return Amount(UNAVAILABLE, reason=missing), Amount(UNAVAILABLE, reason=missing)
    found = [rows[item] for item in query.request_ids if item in rows]
    if len(found) < len(query.request_ids):
        return Amount(PENDING), Amount(PENDING)
    credits = Amount(KNOWN, sum(row.token_credits for row in found))
    starts = [moment for row in found if (moment := _moment(row.start_time)) is not None]
    settled = bool(starts) and now is not None and now - max(starts) >= SQL_CREDIT_LAG
    if not settled and any(row.sql_credits is None for row in found):
        return credits, Amount(PENDING)
    return credits, Amount(KNOWN, sum(row.sql_credits or 0.0 for row in found))


def _judge(query: CostQuery, rows: Mapping[str, JudgeUsage] | str) -> Amount:
    """Count a model's judge tokens, pending until every judge call its runs' spans name has landed."""
    if isinstance(rows, str):
        return Amount(UNAVAILABLE, reason=rows)
    found = [rows[name] for name in query.run_names if name in rows]
    if not found:
        return Amount(UNAVAILABLE, reason="no eval span of its runs names a judge request")
    if len(found) < len(query.run_names) or any(row.landed < row.requests for row in found):
        return Amount(PENDING)
    return Amount(KNOWN, float(sum(row.tokens for row in found)))


def _moment(text: str) -> datetime | None:
    """Read an ISO 8601 time, one with no offset as UTC; None for text that is not one."""
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _first_line(exc: SnowflakePortError) -> str:
    return str(exc).splitlines()[0] if str(exc).strip() else "the usage view could not be read"
