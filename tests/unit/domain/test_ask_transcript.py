"""The document an `sst ask` transcript holds, and the file name it is written under."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.events import ResultTable, RunError, ToolResult, ToolUse, Warning
from snowflake_semantic_tools.domain.ask.transcript import (
    TRANSCRIPT_SCHEMA_VERSION,
    SpecRevision,
    TranscriptHead,
    TurnRecord,
    transcript_document,
    transcript_file_name,
)

AT = "2026-10-19T12:00:00+00:00"
HEAD = TranscriptHead("ab12cd34", AT, "1.0.0", "dev", published=False)


def test_a_transcript_holds_every_turn_whole_and_each_spec_it_ran_on() -> None:
    turn = Turn(
        answer="Seven.",
        thinking="Count them.",
        tool_uses=(ToolUse("t", "SALES", "cortex_analyst_text_to_sql", {"q": "x"}),),
        tool_results=(
            ToolResult("t", "SALES", "cortex_analyst_text_to_sql", "success", "SELECT 7", ResultTable(("N",), ((7,),))),
            ToolResult("u", "DOCS", "cortex_search", "error"),
        ),
        warnings=(Warning("skipped", "W1"),),
        status="completed",
        run_id="9-2",
        tokens=12,
    )
    failed = Turn(error=RunError("refused", "399569", "req"), tokens=3)
    revisions = (
        SpecRevision("agent:a", 1, '{"instructions":{}}', ("semantic_view:s",), AT),
        SpecRevision("agent:a", 2, '{"instructions":{"response":"R"}}', (), AT, ("instructions.response",)),
    )
    turns = (
        TurnRecord("agent:a", 1, "How many?", turn, AT, AT),
        TurnRecord("agent:a", 2, "Again?", failed, AT, AT),
    )
    document = transcript_document(HEAD, revisions, turns, updated_at=AT)
    assert document["schema_version"] == TRANSCRIPT_SCHEMA_VERSION
    assert (document["agents"], document["turn_count"], document["tokens"]) == (["agent:a"], 2, 15)
    turns_written = document["turns"]
    assert isinstance(turns_written, list)
    first, second = turns_written
    assert first["tool_results"][0]["table"] == {"columns": ["N"], "rows": [[7]]}
    assert first["tool_results"][1]["table"] is None and first["error"] is None
    assert first["tool_uses"] == [
        {"tool_use_id": "t", "name": "SALES", "type": "cortex_analyst_text_to_sql", "input": {"q": "x"}}
    ]
    assert first["warnings"] == [{"message": "skipped", "code": "W1"}]
    assert second["error"] == {"message": "refused", "code": "399569", "request_id": "req"}
    later = document["spec_revisions"][1]  # type: ignore[index]
    assert later["spec"] == {"instructions": {"response": "R"}} and later["changes"] == ["instructions.response"]
    assert len(later["spec_sha256"]) == 64
    json.dumps(document)


def test_a_transcript_is_named_for_when_it_started_its_first_agent_and_its_id() -> None:
    assert transcript_file_name(HEAD, "agent:jaffle") == "20261019T120000Z-jaffle-ab12cd34.json"
    assert transcript_file_name(HEAD, "agent:../x") == "20261019T120000Z-%2E.%2Fx-ab12cd34.json"
