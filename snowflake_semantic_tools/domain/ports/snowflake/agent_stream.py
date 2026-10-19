"""Run an agent from a specification over the Cortex Agents REST API, streamed, on a thread.

This is how `sst ask` talks to an agent as it compiles now: `POST /api/v2/cortex/agent:run`
with the whole specification in the body, so no agent object is created for it. A thread keeps
the conversation, so each turn sends one question and the message it follows. A turn runs as a
background run, which is what lets it be cancelled (a synchronous run's cancel is refused as
already completed while it still streams, measured), and a dropped stream resumes after the
last sequence number read. The calls run as the SQL session's own role: with a session token,
Snowflake ignores a role header (measured), so none is sent.

Apart from `SnowflakePort`, so the doubles of one need not implement the other.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.domain.ask.events import AgentEvent

# What every thread SST opens names as its origin, so it can find and sweep its own (16 bytes at most).
ORIGIN = "sst_ask"


@dataclass(frozen=True, slots=True)
class ThreadSummary:
    """One thread the user holds, as listing threads reports it.

    Attributes:
        updated_on: When it last changed, in epoch milliseconds.
    """

    thread_id: int
    origin: str
    updated_on: int


class AgentStreamPort(Protocol):
    """Hold a conversation with an agent built from a specification, publishing nothing."""

    def create_thread(self, origin: str = ORIGIN) -> int:
        """Open a thread, named for `origin`, and return its id.

        Raises:
            SnowflakePortError: Snowflake refused the request, or it could not be sent.
        """
        ...

    def delete_thread(self, thread_id: int) -> bool:
        """Delete a thread and its messages; False when it was already gone.

        Raises:
            SnowflakePortError: Snowflake refused the request, or it could not be sent.
        """
        ...

    def list_threads(self, origin: str = ORIGIN) -> tuple[ThreadSummary, ...]:
        """Return the user's threads opened for `origin`.

        Raises:
            SnowflakePortError: Snowflake refused the request, or it could not be sent.
        """
        ...

    def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
        """Run one turn as `body` says and yield its events as they arrive, unknown ones included.

        `body` is the specification's fields with `messages`, `thread_id` and
        `parent_message_id`; the port adds `stream` and `background`. An HTTP error before the
        stream starts is yielded as one `RunError`, so a run that Snowflake refuses ends the
        same way as one that fails while it streams.

        Raises:
            SnowflakePortError: the request could not be sent, or the stream broke off.
        """
        ...

    def resume(self, run_id: str, starting_after: int) -> Iterator[AgentEvent]:
        """Yield the events of run `run_id` after sequence number `starting_after`, as `run` does.

        Raises:
            SnowflakePortError: the request could not be sent, or the stream broke off.
        """
        ...

    def cancel(self, run_id: str) -> int | None:
        """Stop run `run_id`; return the id of the partial reply it saved, None when it had ended.

        Raises:
            SnowflakePortError: Snowflake refused the request for another reason, or it could
                not be sent.
        """
        ...
