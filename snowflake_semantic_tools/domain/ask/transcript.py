"""A conversation's transcript: every question, what came back, and each specification it ran on.

`sst ask --transcript` keeps one per run of the command, in `sst_target/ask/`, and replaces it
after each turn, so a run that dies keeps what it had. A transcript holds each turn whole --
the question, the answer, the reasoning, each tool call and its result with every row, the
warnings and the error -- and each specification the agent ran on, numbered, so a turn names
the revision it was asked against and a Ctrl-R that changed the agent shows as a new one.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.file_names import file_name

TRANSCRIPT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class TranscriptHead:
    """What a transcript says of the run it records.

    Every time a transcript holds is ISO 8601 text in UTC, as the caller's clock gave it.

    Attributes:
        transcript_id: The run's id, which the file's name ends with.
        started_at: When the run started, `YYYY-MM-DDTHH:MM:SS` first.
        target: The `profiles.yml` target the run connected to.
        published: The run copied nothing, as `--published` asks.
    """

    transcript_id: str
    started_at: str
    sst_version: str
    target: str
    published: bool = False


@dataclass(frozen=True, slots=True)
class SpecRevision:
    """One specification an agent ran on in the run.

    Attributes:
        revision: Its number among the agent's revisions, from 1.
        spec_json: The specification sent, pointed at the copies.
        copied: The key of each dependency copied for it.
        changes: What differs from the agent's previous revision, as `spec_changes` and the
            rebuild name it; empty for the first.
    """

    agent: str
    revision: int
    spec_json: str
    copied: tuple[str, ...]
    recorded_at: str
    changes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TurnRecord:
    """One turn, and the revision of its agent it was asked against."""

    agent: str
    revision: int
    question: str
    turn: Turn
    asked_at: str
    ended_at: str


def transcript_file_name(head: TranscriptHead, agent: str) -> str:
    """Name a run's transcript: when it started, in UTC, the first agent it opened, and its id."""
    stamp = head.started_at[:19].replace("-", "").replace(":", "") + "Z"
    return f"{stamp}-{file_name(agent.removeprefix('agent:'))}-{file_name(head.transcript_id)}.json"


def transcript_document(
    head: TranscriptHead,
    revisions: tuple[SpecRevision, ...],
    turns: tuple[TurnRecord, ...],
    *,
    updated_at: str,
) -> dict[str, object]:
    """Return the transcript as the JSON document the file holds."""
    agents = list(dict.fromkeys((*(item.agent for item in revisions), *(item.agent for item in turns))))
    return {
        "schema_version": TRANSCRIPT_SCHEMA_VERSION,
        "transcript_id": head.transcript_id,
        "sst_version": head.sst_version,
        "target": head.target,
        "published": head.published,
        "started_at": head.started_at,
        "updated_at": updated_at,
        "agents": agents,
        "turn_count": len(turns),
        "tokens": sum(item.turn.tokens for item in turns),
        "spec_revisions": [_revision(item) for item in revisions],
        "turns": [_turn(item) for item in turns],
    }


def _revision(item: SpecRevision) -> dict[str, object]:
    return {
        "agent": item.agent,
        "revision": item.revision,
        "recorded_at": item.recorded_at,
        "changes": list(item.changes),
        "copied": list(item.copied),
        "spec_sha256": hashlib.sha256(item.spec_json.encode("utf-8")).hexdigest(),
        "spec": json.loads(item.spec_json),
    }


def _turn(item: TurnRecord) -> dict[str, object]:
    """Lay one turn out whole: the question, the answer, each tool call and result, and the error."""
    turn = item.turn
    return {
        "agent": item.agent,
        "spec_revision": item.revision,
        "asked_at": item.asked_at,
        "ended_at": item.ended_at,
        "question": item.question,
        "answer": turn.answer,
        "thinking": turn.thinking,
        "status": turn.status,
        "run_id": turn.run_id,
        "tokens": turn.tokens,
        "tool_uses": [
            {"tool_use_id": use.tool_use_id, "name": use.name, "type": use.type, "input": dict(use.input)}
            for use in turn.tool_uses
        ],
        "tool_results": [
            {
                "tool_use_id": result.tool_use_id,
                "name": result.name,
                "type": result.type,
                "status": result.status,
                "sql": result.sql,
                "semantic_model": result.semantic_model,
                "table": (
                    {"columns": list(result.table.columns), "rows": [list(row) for row in result.table.rows]}
                    if result.table is not None
                    else None
                ),
            }
            for result in turn.tool_results
        ],
        "warnings": [{"message": warning.message, "code": warning.code} for warning in turn.warnings],
        "error": (
            {"message": turn.error.message, "code": turn.error.code, "request_id": turn.error.request_id}
            if turn.error is not None
            else None
        ),
    }
