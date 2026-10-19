"""One `sst ask` session: sweep what older sessions left, copy what you edited, and talk.

`start_session` does what a session needs before its first question: removes the scratch
copies and threads older sessions left behind, copies each dependency the agent reaches that
compiles differently from what the target's state records (unless asked to run the published
ones, which SST-ASK001 then warns of), and points the agent's specification at the copies.
`rebuild` does it again after the project changed, keeping the thread, so the conversation goes
on with the new edits, and says what changed. `end_session` deletes the thread and drops every
copy, however the session ended.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from snowflake_semantic_tools.app.ask import stale_dependencies, unpublished_reason
from snowflake_semantic_tools.app.ask_conversation import AskConversation
from snowflake_semantic_tools.app.ask_scratch import (
    Edited,
    ScratchPort,
    ScratchSession,
    Swept,
    edited_dependencies,
    sweep,
)
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.domain.ask.spec import rewrite_spec, spec_changes
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import AgentStreamPort
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry


class AskPort(ScratchPort, AgentStreamPort, Protocol):
    """What a session uses: statements and uploads for its copies, and the REST conversation."""


@dataclass
class AskSession:
    """A session in progress: its agent, its copies, its conversation, and what it found starting.

    Attributes:
        notes: SST-ASK001 for each published dependency a `published` session calls in place
            of your edit; none otherwise.
        swept: What starting removed of older sessions.
        agent_state: How the agent itself stands against what the target publishes, as
            `agent_edited` says it; None when the target publishes it as it compiles.
    """

    agent: CompiledAgent
    scope: SchemaScope
    scratch: ScratchSession
    conversation: AskConversation
    edited: Edited
    notes: tuple[Diagnostic, ...] = ()
    swept: Swept = Swept()
    published: bool = False
    agent_state: str | None = None


def start_session(
    port: AskPort,
    agent: CompiledAgent,
    compiled: CompileResult,
    applied: Mapping[str, AppliedEntry] | None,
    *,
    scope: SchemaScope,
    session: str,
    now: datetime,
    published: bool = False,
    progress: Callable[[str], None] = lambda line: None,
) -> AskSession | Diagnostic:
    """Sweep, copy, and point the agent at the copies; the failure when a copy is refused.

    Args:
        published: Copy nothing, and warn of each edited dependency the agent still calls.
        session: The session's id, which each copy's name ends with.

    Diagnostics:
        SST-ASK008: a copy was refused; nothing this session made is left.
        SST-ASK001, SST-ASK009: as `stale_dependencies` and `sweep` report them.
    """
    progress("removing what earlier sessions left")
    swept = sweep(port, port, (scope,), now=now, keep=session)
    scratch = ScratchSession(port, scope, session, progress)
    if published:
        edited, notes = Edited(()), stale_dependencies(agent, compiled, applied)
    else:
        edited, notes = edited_dependencies(agent, compiled, applied), ()
        failed = scratch.build(edited)
        if failed is not None:
            return failed
    spec, _ = rewrite_spec(agent.payload, scratch.renames)
    conversation = AskConversation(port, agent.artifact_key, spec)
    state = agent_edited(agent, applied)
    return AskSession(agent, scope, scratch, conversation, edited, notes, swept, published, state)


def agent_edited(agent: CompiledAgent, applied: Mapping[str, AppliedEntry] | None) -> str | None:
    """Say how the agent itself stands against the target; None when it publishes as it compiles.

    A session always runs the agent's specification as it compiles now, so this only tells you
    that the conversation is not the published agent's: `agent spec edited`, `agent not
    published`, or `agent's published state unknown` when the state could not be read.
    """
    rendered = agent.rendered_artifact
    if unpublished_reason(applied, agent.artifact_key, rendered.fingerprint, rendered.target) is None:
        return None
    if applied is None:
        return "agent's published state unknown"
    entry = applied.get(agent.artifact_key)
    return "agent not published" if entry is None or entry.outcome != APPLIED else "agent spec edited"


@dataclass(frozen=True, slots=True)
class Rebuilt:
    """What `rebuild` changed.

    Attributes:
        spec: What of the agent's own specification differs from the one the conversation ran,
            as `spec_changes` names it: `instructions.response`, `tools`, and so on.
        dependencies: Each dependency whose copy is new, now compiles differently, or is gone.
        failed: SST-ASK008 when a copy was refused; the conversation then runs the agent's
            specification as it compiles, pointed at the published objects, with no copies.
        left: SST-ASK009 for each old copy that could not be dropped first.
    """

    spec: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    failed: Diagnostic | None = None
    left: tuple[Diagnostic, ...] = ()

    @property
    def changed(self) -> bool:
        """Report whether the agent or anything copied for it changed."""
        return bool(self.spec or self.dependencies)


def rebuild(
    session: AskSession, agent: CompiledAgent, compiled: CompileResult, applied: Mapping[str, AppliedEntry] | None
) -> Rebuilt:
    """Copy again what the project now edits, point the conversation at it, and say what changed.

    Every copy is dropped and made again, so a dependency no longer edited stops being copied.
    The thread stays.

    Diagnostics:
        SST-ASK008: a copy was refused; the conversation keeps no copies, and runs the agent as
            it compiles against what is published.
        SST-ASK009: an old copy could not be dropped before the new ones were made.
    """
    before = _fingerprints(session.edited)
    changes = spec_changes(session.agent.payload, agent.payload)
    left = session.scratch.drop()
    session.agent = agent
    session.agent_state = agent_edited(agent, applied)
    session.conversation.artifact_key = agent.artifact_key
    failed = None
    if session.published:
        session.notes = stale_dependencies(agent, compiled, applied)
    else:
        session.edited = edited_dependencies(agent, compiled, applied)
        failed = session.scratch.build(session.edited)
        if failed is not None:
            session.edited = Edited(())
    after = _fingerprints(session.edited)
    moved = tuple(key for key in dict.fromkeys((*before, *after)) if before.get(key) != after.get(key))
    session.conversation.spec_json, _ = rewrite_spec(agent.payload, session.scratch.renames)
    return Rebuilt(changes, moved, failed, left)


def _fingerprints(edited: Edited) -> dict[str, str]:
    return {item.artifact_key: item.rendered_artifact.fingerprint for item in edited.artifacts}


def end_session(session: AskSession) -> tuple[Diagnostic, ...]:
    """Delete the thread and drop every copy; what could not be removed is SST-ASK009.

    Diagnostics:
        SST-ASK009: the thread or a copy could not be removed.
    """
    return (*session.conversation.close(), *session.scratch.drop())
