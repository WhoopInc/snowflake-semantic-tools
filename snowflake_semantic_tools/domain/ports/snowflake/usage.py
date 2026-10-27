"""The usage a comparison's runs left: the agent's credits by request, and the judges' tokens by run.

Snowflake records an agent request's cost in `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY`,
one row per request, keyed by the REQUEST_ID an evaluation's results report for each question.
A judge's calls are in `CORTEX_REST_API_USAGE_HISTORY`, which has tokens and no credits, keyed by
the request ids an evaluation's `eval` spans list. Both views lag: a row may land an hour after
its request, and an agent row's SQL credits up to eight hours after
(docs/design/snowflake-behavior.md). The port is apart from `SnowflakePort`, so the doubles of
one need not implement the other.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AgentUsage:
    """One agent request's row of `CORTEX_AGENT_USAGE_HISTORY`.

    Attributes:
        token_credits: TOKEN_CREDITS: the credits the request's model tokens cost.
        sql_credits: METADATA's `sql_query_credits`; None while it is null, as it often is.
        start_time: START_TIME, ISO 8601 with offset.
    """

    request_id: str
    token_credits: float
    sql_credits: float | None
    start_time: str


@dataclass(frozen=True, slots=True)
class JudgeUsage:
    """What the judges of one evaluation run used, as its eval spans and the REST usage view tell it.

    Attributes:
        requests: The distinct judge requests the run's `eval` spans name.
        landed: How many of them have a `CORTEX_REST_API_USAGE_HISTORY` row yet.
        tokens: The TOKENS of those that landed.
    """

    run_name: str
    requests: int
    landed: int
    tokens: int


class UsagePort(Protocol):
    """Read what a comparison's runs used, from the account's usage views."""

    def agent_usage(self, request_ids: Sequence[str], *, since: str) -> tuple[AgentUsage, ...]:
        """Return the agent usage rows of these requests that have landed, in any order.

        A request with no row yet is absent. Never writes.

        Args:
            since: No request started before this ISO 8601 time; it bounds what the view scans.

        Raises:
            SnowflakePortError: the read failed, as for a role that may not read ACCOUNT_USAGE.
        """
        ...

    def judge_usage(self, run_names: Sequence[str]) -> tuple[JudgeUsage, ...]:
        """Return the judges' usage of each named evaluation run whose eval spans have landed.

        A run with no eval span is absent. Never writes.

        Raises:
            SnowflakePortError: the read failed, as for a role that may not read the events table
                or ACCOUNT_USAGE.
        """
        ...
