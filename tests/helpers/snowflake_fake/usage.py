"""The fake's `UsagePort`: usage rows a test lands, read as the usage views return them.

`agent_usage_rows` holds one row per agent request that has landed, by request id, and
`judge_usage_rows` one per evaluation run whose eval spans have, by run name; a request or run
absent from them has not landed yet. Each read is logged as a query, so a test sees it was made,
and `fail` arms either to raise, as for a role that may not read ACCOUNT_USAGE.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage, JudgeUsage
from tests.helpers.snowflake_fake.world import Sent, SnowflakeWorld


class FakeUsage(SnowflakeWorld):
    """The usage reads: `agent_usage_rows` by request id, `judge_usage_rows` by run name."""

    @property
    def agent_usage_rows(self) -> dict[str, AgentUsage]:
        rows: dict[str, AgentUsage] = self.__dict__.setdefault("_agent_usage_rows", {})
        return rows

    @property
    def judge_usage_rows(self) -> dict[str, JudgeUsage]:
        rows: dict[str, JudgeUsage] = self.__dict__.setdefault("_judge_usage_rows", {})
        return rows

    def agent_usage(self, request_ids: Sequence[str], *, since: str) -> tuple[AgentUsage, ...]:
        self.log.append(Sent("query", ("agent_usage",), (since, tuple(sorted(request_ids)))))
        self._check("agent_usage")
        return tuple(self.agent_usage_rows[item] for item in sorted(request_ids) if item in self.agent_usage_rows)

    def judge_usage(self, run_names: Sequence[str]) -> tuple[JudgeUsage, ...]:
        self.log.append(Sent("query", ("judge_usage",), tuple(sorted(run_names))))
        self._check("judge_usage")
        return tuple(self.judge_usage_rows[item] for item in sorted(run_names) if item in self.judge_usage_rows)
