"""The fake's `AgentRunPort`: every temporary agent answers each question with a scripted reply.

By default a question is answered with `answer to <question>`, calling no tool; a test of
`sst ask` or of unpublished evals sets `agent_replies` to script a reply, or a failure, per
question.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.world import SnowflakeWorld


class FakeAgentRun(SnowflakeWorld):
    """Answer each inline run from `agent_replies`, by question; record every run asked.

    Attributes:
        agent_replies: The reply, or the error raised, for each question; None, or a question
            it lacks, answers `answer to <question>`.
        inline_runs: Every run asked, as (scope, agent, spec_json, question, timeout), in order.
    """

    agent_replies: dict[str, AgentReply | SnowflakePortError] | None = None

    @property
    def inline_runs(self) -> list[tuple[SchemaScope, str, str, str, int]]:
        """Every inline run asked so far, in order."""
        runs: list[tuple[SchemaScope, str, str, str, int]] = self.__dict__.setdefault("_inline_runs", [])
        return runs

    def run_inline(self, scope: SchemaScope, agent: str, spec_json: str, question: str, timeout: int) -> AgentReply:
        self._check("run_inline")
        self.inline_runs.append((scope, agent, spec_json, question, timeout))
        answer = (self.agent_replies or {}).get(question)
        if isinstance(answer, SnowflakePortError):
            raise answer
        return answer if answer is not None else AgentReply(f"answer to {question}")
