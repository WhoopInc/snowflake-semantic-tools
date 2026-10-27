"""A conversation with an agent on a thread: what each turn adds up to, and what the next needs.

A thread keeps the conversation on Snowflake's side, so each turn sends one question and the
id of the reply it follows: 0 for the first, then the last reply's assistant message id, which
the run's final `response` carries (and, when that is missing, the last reply that had one, as
the Threads page says). A turn's run id names it for cancel and resume; its last sequence
number is where a resume starts after.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from snowflake_semantic_tools.domain.ask.events import (
    AgentEvent,
    Finished,
    MessageIds,
    RunError,
    TextDelta,
    ThinkingDelta,
    ToolResult,
    ToolUse,
    Warning,
)

COMPLETED, CANCELLED, TIMED_OUT = "completed", "cancelled", "timed_out"


@dataclass(frozen=True, slots=True)
class Turn:
    """What one turn's events added up to so far.

    Attributes:
        status: The final `response`'s status; empty while the run goes on.
        run_id: The run's id, once an event has carried it.
        assistant_message_id: The reply's message id, which the next turn follows.
        last_sequence: The highest sequence number seen, which a resume starts after.
        tokens: The tokens the run used, from its final usage.
    """

    answer: str = ""
    thinking: str = ""
    tool_uses: tuple[ToolUse, ...] = ()
    tool_results: tuple[ToolResult, ...] = ()
    warnings: tuple[Warning, ...] = ()
    error: RunError | None = None
    status: str = ""
    run_id: str = ""
    assistant_message_id: int | None = None
    last_sequence: int = 0
    tokens: int = 0

    @property
    def finished(self) -> bool:
        """Report whether the run ended, by a final response or an error."""
        return bool(self.status) or self.error is not None


def add(turn: Turn, event: AgentEvent) -> Turn:
    """Return the turn with `event` added; an event that says nothing a turn keeps leaves it as it was."""
    sequence = getattr(event, "sequence", None)
    if isinstance(sequence, int) and sequence > turn.last_sequence:
        turn = replace(turn, last_sequence=sequence)
    match event:
        case TextDelta(text=text):
            return replace(turn, answer=turn.answer + text)
        case ThinkingDelta(text=text):
            return replace(turn, thinking=turn.thinking + text)
        case ToolUse():
            return replace(turn, tool_uses=(*turn.tool_uses, event))
        case ToolResult():
            return replace(turn, tool_results=(*turn.tool_results, event))
        case Warning():
            return replace(turn, warnings=(*turn.warnings, event))
        case RunError():
            return replace(turn, error=event)
        case MessageIds(role="user", run_id=run_id) if run_id:
            return replace(turn, run_id=run_id)
        case MessageIds(role="assistant", message_id=message_id):
            return replace(turn, assistant_message_id=message_id)
        case Finished():
            return replace(
                turn,
                status=event.status,
                run_id=event.run_id or turn.run_id,
                assistant_message_id=event.assistant_message_id or turn.assistant_message_id,
                tokens=event.tokens,
            )
    return turn


@dataclass(frozen=True, slots=True)
class Thread:
    """A conversation's place on its thread.

    Attributes:
        parent: The message the next question follows: 0 before the first reply.
        turns: How many turns have finished.
        tokens: Tokens every finished turn used.
    """

    thread_id: int
    parent: int = 0
    turns: int = 0
    tokens: int = 0

    def after(self, turn: Turn) -> Thread:
        """Return the thread once `turn` ended: the next question follows its reply, when it has one."""
        parent = turn.assistant_message_id if turn.assistant_message_id else self.parent
        return replace(self, parent=parent, turns=self.turns + 1, tokens=self.tokens + turn.tokens)


def run_id(thread_id: int, user_message_id: int) -> str:
    """Return a run's id as the Run API defines it: the thread's id, then the question's message id."""
    return f"{thread_id}-{user_message_id}"
