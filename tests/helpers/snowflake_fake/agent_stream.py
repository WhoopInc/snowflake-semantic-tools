"""The fake's `AgentStreamPort`: threads in memory, and each question's events scripted.

A run answers from `agent_streams` when the question is there: a list of events, or an error
the run raises. Otherwise it answers from `agent_replies`, as the inline run does: its text as
one delta, each tool call as a `ToolUse`, and an error as a `RunError`; and with neither, `answer
to <question>`. Every run's events start with the question's message ids, as a background run's
do, and end with the final response and `done`.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.domain.ask.events import (
    AgentEvent,
    Done,
    Finished,
    MessageIds,
    RunError,
    Status,
    TextDelta,
    ToolUse,
)
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import ORIGIN, ThreadSummary
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.agent_run import FakeAgentRun


class FakeAgentStream(FakeAgentRun):
    """Hold threads and answer streamed runs as scripted; record every run's body.

    Attributes:
        agent_streams: Each question's events, or the error its run raises.
        stale_threads: Threads an earlier session left, as `list_threads` reports them.
        cancelled: The run ids cancelled, in order.
    """

    agent_streams: dict[str, list[AgentEvent] | SnowflakePortError] | None = None
    stale_threads: tuple[ThreadSummary, ...] = ()

    @property
    def threads(self) -> dict[int, str]:
        """The threads open now, by id, with their origin."""
        found: dict[int, str] = self.__dict__.setdefault("_threads", {})
        return found

    @property
    def deleted_threads(self) -> list[int]:
        """Every thread deleted, in order."""
        found: list[int] = self.__dict__.setdefault("_deleted_threads", [])
        return found

    @property
    def stream_runs(self) -> list[Mapping[str, object]]:
        """Every run's body, in order."""
        found: list[Mapping[str, object]] = self.__dict__.setdefault("_stream_runs", [])
        return found

    @property
    def cancelled(self) -> list[str]:
        """Every run id cancelled, in order."""
        found: list[str] = self.__dict__.setdefault("_cancelled", [])
        return found

    def create_thread(self, origin: str = ORIGIN) -> int:
        self._check("create_thread")
        thread_id = 100 + len(self.threads) + len(self.deleted_threads)
        self.threads[thread_id] = origin
        return thread_id

    def delete_thread(self, thread_id: int) -> bool:
        self._check("delete_thread")
        self.deleted_threads.append(thread_id)
        return self.threads.pop(thread_id, None) is not None or any(
            thread.thread_id == thread_id for thread in self.stale_threads
        )

    def list_threads(self, origin: str = ORIGIN) -> tuple[ThreadSummary, ...]:
        self._check("list_threads")
        return tuple(thread for thread in self.stale_threads if thread.thread_id not in self.deleted_threads)

    def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
        self._check("run")
        self.stream_runs.append(body)
        question = _question(body)
        thread = int(str(body.get("thread_id") or 0))
        user = 1000 + 2 * len(self.stream_runs)
        run_id = f"{thread}-{user}"
        scripted = (self.agent_streams or {}).get(question)
        if isinstance(scripted, SnowflakePortError):
            raise scripted
        yield MessageIds("user", user, run_id, 0)
        events = scripted if scripted is not None else self._from_reply(question)
        yield from events
        if not any(isinstance(event, (Finished, RunError)) for event in events):
            yield MessageIds("assistant", user + 1, "", 98)
            yield Finished("completed", run_id, user + 1, 10, 99)
        yield Done()

    def _from_reply(self, question: str) -> list[AgentEvent]:
        reply = (self.agent_replies or {}).get(question)
        if isinstance(reply, SnowflakePortError):
            raise reply
        if reply is None:
            return [Status("planning", "Planning the next steps", 1), TextDelta(f"answer to {question}", 2)]
        if reply.error is not None:
            return [RunError(reply.error, "")]
        uses: list[AgentEvent] = [
            ToolUse(f"tool-{index}", use.name, use.type, use.input, index + 1)
            for index, use in enumerate(reply.tool_uses)
        ]
        return [*uses, TextDelta(reply.answer, 50)]

    def resume(self, run_id: str, starting_after: int) -> Iterator[AgentEvent]:
        self._check("resume")
        yield Finished("completed", run_id, None, 0, starting_after + 1)

    def cancel(self, run_id: str) -> int | None:
        self._check("cancel")
        self.cancelled.append(run_id)
        return 7


def _question(body: Mapping[str, object]) -> str:
    messages = body.get("messages")
    first = messages[0] if isinstance(messages, list) and messages else {}
    content = first.get("content") if isinstance(first, dict) else None
    item = content[0] if isinstance(content, list) and content else {}
    return str(item.get("text") or "") if isinstance(item, dict) else ""
