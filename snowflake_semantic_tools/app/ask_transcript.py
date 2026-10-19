"""Keep a run's transcript as it goes, for `sst ask --transcript`.

The recorder holds what `domain.ask.transcript` lays out and has the store replace the file
whenever there is more: once the agent's specification is set (a session starts, or Ctrl-R
changed it), and after each turn. A transcript that cannot be written is SST-ASK012 once,
a warning: the conversation goes on, and later writes are tried again, quietly.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from snowflake_semantic_tools.app.ask_session import AskSession
from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.transcript import (
    SpecRevision,
    TranscriptHead,
    TurnRecord,
    transcript_document,
    transcript_file_name,
)
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic


class TranscriptStore(Protocol):
    """Where transcripts are kept."""

    def write(self, name: str, document: Mapping[str, object]) -> str:
        """Replace the transcript `name` with `document`, and return where it is.

        Raises:
            OSError: The file could not be written.
        """
        ...


@dataclass
class TranscriptRecorder:
    """One run's transcript, written again each time it grows.

    Attributes:
        path: Where the store last wrote it; None before the first write succeeds.
    """

    store: TranscriptStore
    head: TranscriptHead
    clock: Callable[[], datetime]
    path: str | None = None
    revisions: list[SpecRevision] = field(default_factory=list)
    turns: list[TurnRecord] = field(default_factory=list)
    _warned: bool = False

    def spec(self, session: AskSession, changes: tuple[str, ...] = ()) -> Diagnostic | None:
        """Record the specification `session` now runs, unless it is its agent's latest, and write.

        Diagnostics:
            SST-ASK012: as `save` reports it.
        """
        self._revision(session, changes)
        return self.save()

    def turn(self, session: AskSession, question: str, turn: Turn, asked_at: datetime) -> Diagnostic | None:
        """Record a turn that ended, against the revision its agent ran, and write.

        Diagnostics:
            SST-ASK012: as `save` reports it.
        """
        revision = self._revision(session, ())
        agent = session.agent.artifact_key
        self.turns.append(TurnRecord(agent, revision, question, turn, asked_at.isoformat(), self._now()))
        return self.save()

    def save(self) -> Diagnostic | None:
        """Replace the file with what is recorded; nothing before the first agent opens.

        Diagnostics:
            SST-ASK012: the first time the file cannot be written; later failures say nothing.
        """
        if not self.revisions:
            return None
        name = transcript_file_name(self.head, self.revisions[0].agent)
        document = transcript_document(self.head, tuple(self.revisions), tuple(self.turns), updated_at=self._now())
        try:
            self.path = self.store.write(name, document)
        except OSError as exc:
            if self._warned:
                return None
            self._warned = True
            return D("SST-ASK012", value=name, detail=str(exc))
        return None

    def _now(self) -> str:
        return self.clock().isoformat()

    def _revision(self, session: AskSession, changes: tuple[str, ...]) -> int:
        """Return the number of the revision `session` runs, recording it first when it is new."""
        agent = session.agent.artifact_key
        latest = next((item for item in reversed(self.revisions) if item.agent == agent), None)
        spec, copied = session.conversation.spec_json, session.edited.keys
        if latest is not None and latest.spec_json == spec and latest.copied == copied:
            return latest.revision
        number = latest.revision + 1 if latest is not None else 1
        self.revisions.append(SpecRevision(agent, number, spec, copied, self._now(), changes))
        return number
