"""Compiled agents, state entries and a scripted `AgentRunPort`, for `sst ask` and the unpublished evals.

`compiled_agent` builds an agent whose tools depend on the artifact keys given, published as
`DB.AGENTS.<NAME>`; `applied_entry` records one artifact in state as published, as compiled
unless told otherwise; `ScriptedAgentRun` answers each question from a script.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgent, ResolvedAgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry


def compiled_agent(
    name: str,
    *depends_on: str,
    settings: Mapping[str, object] | None = None,
    payload: str = "{}\n",
    orchestration_model: str = "auto",
) -> CompiledAgent:
    """An agent with one generic tool per artifact key in `depends_on`, each depending on that key."""
    model = AgentModel(
        name,
        Origin(f"{name}.yml"),
        (f"{name}.yml",),
        orchestration_model=orchestration_model,
        settings=MappingProxyType({key: Setting(key, value, "root") for key, value in (settings or {}).items()}),
    )
    tools = tuple(
        ResolvedAgentTool("generic", f"tool_{index}", "", depends_on=(key,)) for index, key in enumerate(depends_on)
    )
    return CompiledAgent(ResolvedAgent(model, tools), QualifiedName.from_parts("db", "agents", name), payload, name * 4)


def applied_entry(
    agent: CompiledAgent, *, outcome: str = APPLIED, fingerprint: str | None = None, target: str = ""
) -> AppliedEntry:
    """State's entry for `agent` as published: its own fingerprint and target unless given others."""
    published = fingerprint or agent.definition_fingerprint
    return AppliedEntry(published, target or agent.target.sql, "now", "run", outcome, published, "m")


class ScriptedAgentRun:
    """Answer each question from `replies`, else `answer to <question>`; record every run asked.

    Attributes:
        asked: Every run, as (scope, agent, spec_json, question, timeout), in order.
    """

    def __init__(self, replies: Mapping[str, AgentReply | SnowflakePortError] | None = None) -> None:
        self._replies = dict(replies or {})
        self.asked: list[tuple[SchemaScope, str, str, str, int]] = []

    def run_inline(self, scope: SchemaScope, agent: str, spec_json: str, question: str, timeout: int) -> AgentReply:
        self.asked.append((scope, agent, spec_json, question, timeout))
        answer = self._replies.get(question)
        if isinstance(answer, SnowflakePortError):
            raise answer
        return answer if answer is not None else AgentReply(f"answer to {question}")
