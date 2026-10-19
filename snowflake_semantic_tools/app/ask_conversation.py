"""One `sst ask` conversation: a thread, the agent on its scratch copies, and each turn streamed.

`AskConversation` opens the thread, sends each question as the next turn and yields its events
as they arrive, folding them into the `Turn` the caller reads once the run ends. A turn whose
stream breaks off resumes after the last sequence number read, once. Ctrl-C cancels the turn in
flight: the partial reply is saved to the thread, and the next question follows it. Closing
deletes the thread. Nothing here creates an object; `ScratchSession` makes the copies the
specification names.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.ask.conversation import Thread, Turn, add
from snowflake_semantic_tools.domain.ask.events import AgentEvent, MessageIds, RunError
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import ORIGIN, AgentStreamPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError, SnowflakeTransientError

# The error code Snowflake refuses a run with when `tool_not_accessible: reject` finds a tool it cannot reach.
TOOL_NOT_ACCESSIBLE = "399569"


@dataclass
class AskConversation:
    """A conversation with one agent on one thread.

    Args:
        artifact_key: The agent's key, as diagnostics name it.
        spec_json: The agent's specification, already pointed at its scratch copies.
    """

    stream: AgentStreamPort
    artifact_key: str
    spec_json: str
    thread: Thread | None = None
    turn: Turn = field(default_factory=Turn)
    # A cancel asked for before the run's id arrived, sent as soon as it does.
    _cancel_pending: bool = False

    def open(self) -> None:
        """Open the conversation's thread.

        Raises:
            SnowflakePortError: the thread could not be created.
        """
        self.thread = Thread(self.stream.create_thread(ORIGIN))

    def ask(self, question: str) -> Iterator[AgentEvent]:
        """Send `question` as the next turn and yield its events; `turn` holds what they add up to.

        A stream that breaks off before the run ended is resumed once, after the last sequence
        number read. The thread then follows the turn's reply.

        Raises:
            SnowflakePortError: the run could not be sent, or broke off twice.
        """
        if self.thread is None:
            self.open()
        assert self.thread is not None
        self.turn = Turn()
        self._cancel_pending = False
        body = {
            **_spec_fields(self.spec_json),
            "thread_id": self.thread.thread_id,
            "parent_message_id": self.thread.parent,
            "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
        }
        try:
            yield from self._read(self.stream.run(body))
        except SnowflakeTransientError:
            if not self.turn.run_id or self.turn.finished:
                raise
            yield from self._read(self.stream.resume(self.turn.run_id, self.turn.last_sequence))
        self.thread = self.thread.after(self.turn)

    def _read(self, events: Iterator[AgentEvent]) -> Iterator[AgentEvent]:
        for event in events:
            if isinstance(event, MessageIds) and event.role == "user" and not event.run_id and self.thread:
                event = MessageIds(
                    event.role, event.message_id, f"{self.thread.thread_id}-{event.message_id}", event.sequence
                )
            self.turn = add(self.turn, event)
            if self._cancel_pending and self.turn.run_id and not self.turn.finished:
                self._cancel_pending = False
                self.cancel()
            yield event

    def cancel(self) -> bool:
        """Cancel the turn in flight; False when there is none, or it had already ended.

        A turn whose run id has not arrived yet is cancelled as soon as it does. The thread
        follows the partial reply the cancel saved.
        """
        if self.turn.finished:
            return False
        if not self.turn.run_id:
            self._cancel_pending = self.thread is not None
            return False
        try:
            saved = self.stream.cancel(self.turn.run_id)
        except SnowflakePortError:
            return False
        if saved is not None and self.thread is not None:
            self.thread = Thread(self.thread.thread_id, saved, self.thread.turns, self.thread.tokens)
        return saved is not None

    def close(self) -> tuple[Diagnostic, ...]:
        """Delete the thread, if one was opened.

        Diagnostics:
            SST-ASK009: the thread could not be deleted.
        """
        if self.thread is None:
            return ()
        thread_id, self.thread = self.thread.thread_id, None
        try:
            self.stream.delete_thread(thread_id)
        except SnowflakePortError as exc:
            return (D("SST-ASK009", value=f"thread {thread_id}", detail=str(exc).splitlines()[0]),)
        return ()


def turn_diagnostics(artifact_key: str, turn: Turn) -> tuple[Diagnostic, ...]:
    """Report a turn that failed: a tool it cannot reach as SST-ASK010, any other error as SST-ASK002.

    Diagnostics:
        SST-ASK010: Snowflake refused the run because a tool is missing or unreachable.
        SST-ASK002: the run failed, or ended other than completed or cancelled.
    """
    error = turn.error
    if isinstance(error, RunError):
        code = "SST-ASK010" if error.code == TOOL_NOT_ACCESSIBLE else "SST-ASK002"
        detail = f"{error.message}{f' (request {error.request_id})' if error.request_id else ''}"
        return (D(code, subject=artifact_key, artifact=artifact_key, detail=detail),)
    if turn.status not in ("", "completed", "cancelled"):
        return (D("SST-ASK002", subject=artifact_key, artifact=artifact_key, detail=f"the run ended {turn.status}"),)
    return ()


def _spec_fields(spec_json: str) -> dict[str, object]:
    """Return the specification's fields, which a run's body carries as they compiled (measured)."""
    spec = json.loads(spec_json)
    return dict(spec) if isinstance(spec, dict) else {}
