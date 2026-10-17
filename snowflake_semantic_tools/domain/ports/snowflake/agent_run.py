"""Run an agent from a specification, without publishing it: what `sst ask` and unpublished evals call.

A run creates a TEMPORARY agent from the specification, asks it one question through
`DATA_AGENT_RUN`, and drops it, all on one session, since a temporary agent exists in its
session alone. Nothing it does is recorded in state or visible to another session. This port
is apart from `SnowflakePort`, so the doubles of one need not implement the other.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import SchemaScope


@dataclass(frozen=True, slots=True)
class ToolUse:
    """One tool call the agent made, as the run's `tool_use` item reports it.

    Attributes:
        name: The tool's name. Snowflake also reports calls of its own system tools, such as
            `server_skill` and `system_execute_sql`, which the specification does not declare.
        input: The call's input, as the run reports it; empty when it reports none.
    """

    name: str
    type: str
    input: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class ToolResult:
    """What one tool call returned, as the run's `tool_result` item reports it: its name and status."""

    name: str
    status: str


@dataclass(frozen=True, slots=True)
class AgentReply:
    """What one agent run answered.

    Attributes:
        answer: The reply's text items joined by blank lines; empty when it has none.
        tool_uses: Every tool call, in reply order, system tools included.
        tool_results: Every tool result, in reply order.
        error: Snowflake's message when the run answered with an error instead of a reply;
            None when it replied.
        duration_ms: How long the run took, from asking to the answer; 0 when not measured.
    """

    answer: str = ""
    tool_uses: tuple[ToolUse, ...] = ()
    tool_results: tuple[ToolResult, ...] = ()
    error: str | None = None
    duration_ms: int = 0


class AgentRunPort(Protocol):
    """Ask an agent built from a specification one question, publishing nothing."""

    def run_inline(self, scope: SchemaScope, agent: str, spec_json: str, question: str, timeout: int) -> AgentReply:
        """Create a temporary agent from `spec_json` in `scope`, ask it `question`, and drop it.

        The temporary agent is named for `agent`, with a random suffix, so concurrent runs
        never share one. The drop is attempted however the run ends; a temporary agent left
        behind ends with the session. Writes nothing else and records nothing.

        Args:
            spec_json: The agent's rendered specification, as `sst compile --emit-agent-spec`
                writes it.
            timeout: Seconds the run may take before the driver cancels it.

        Returns:
            The reply; one with `error` set when the run answered with an error message.

        Raises:
            SnowflakePortError: the agent could not be created, or the run statement failed
                or timed out.
            ValueError: the specification holds `$$`, which would end the literal it is
                inlined in.
        """
        ...
