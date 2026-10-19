"""The pure half of `sst ask`: the specification on scratch copies, the event stream, and the conversation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.domain.ask.conversation import Thread, Turn, add, run_id
from snowflake_semantic_tools.domain.ask.events import (
    AgentEvent,
    Chart,
    Done,
    Finished,
    MessageIds,
    ResultTable,
    RunError,
    SseDecoder,
    SseEvent,
    Status,
    Suggestions,
    TextDelta,
    ThinkingDelta,
    ToolProgress,
    ToolResult,
    ToolUse,
    Unknown,
    Warning,
    decode,
    result_table,
)
from snowflake_semantic_tools.domain.ask.spec import Rename, referenced_names, rewrite_spec, spec_changes
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.sql import literal, qname, renamed, sql

_SPEC: dict[str, Any] = {
    "models": {"orchestration": "auto"},
    "tools": [{"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "SALES"}}],
    "tool_resources": {
        "SALES": {"semantic_view": "DB.SV.SALES", "execution_environment": {"type": "warehouse"}},
        "search": {"search_service": "DB.T.SEARCH"},
        "lookup": {"identifier": "DB.T.LOOKUP", "type": "function"},
        "helper": {"agent_name": "DB.A.HELPER"},
        "odd": "not an object",
    },
    "skills": [
        {"name": "s", "source": {"type": "CORTEX_EXTENSION", "path": "DB.SK.S", "version": "SST_1"}},
        {"name": "t", "source": {"type": "CORTEX_EXTENSION", "path": "DB.SK.T", "version": "SST_2"}},
        "not an object",
    ],
}


def test_each_renamed_reference_points_at_its_copy_and_inaccessible_tools_are_rejected() -> None:
    renames = {
        "db.sv.sales": Rename("SCR.X.SALES__SST_ASK_1"),
        "db.t.lookup": Rename("SCR.X.LOOKUP__SST_ASK_1"),
        "db.a.helper": Rename("SCR.X.HELPER__SST_ASK_1"),
        "db.sk.s": Rename("SCR.X.S__SST_ASK_1", "SST_9"),
    }
    text, done = rewrite_spec(json.dumps(_SPEC), renames)
    spec = json.loads(text)
    resources = spec["tool_resources"]
    assert resources["SALES"]["semantic_view"] == "SCR.X.SALES__SST_ASK_1"
    assert resources["search"]["search_service"] == "DB.T.SEARCH"
    assert resources["lookup"]["identifier"] == "SCR.X.LOOKUP__SST_ASK_1"
    assert resources["helper"]["agent_name"] == "SCR.X.HELPER__SST_ASK_1"
    assert spec["skills"][0]["source"] == {"type": "CORTEX_EXTENSION", "path": "SCR.X.S__SST_ASK_1", "version": "SST_9"}
    assert spec["skills"][1]["source"]["path"] == "DB.SK.T"
    assert spec["orchestration"] == {"tool_not_accessible": "reject"}
    assert done == ("db.sv.sales", "db.t.lookup", "db.a.helper", "db.sk.s")
    # An agent that already says what to do with an inaccessible tool keeps its choice.
    kept = json.loads(
        rewrite_spec(json.dumps({**_SPEC, "orchestration": {"tool_not_accessible": "accept"}}), renames)[0]
    )
    assert kept["orchestration"] == {"tool_not_accessible": "accept"}
    # A version-less rename keeps the version, and nothing renamed changes nothing.
    same = rewrite_spec(json.dumps(_SPEC), {"db.sk.t": Rename("SCR.X.T")})[0]
    assert json.loads(same)["skills"][1]["source"]["version"] == "SST_2"
    assert rewrite_spec(json.dumps(_SPEC), {"other.x.y": Rename("A.B.C")})[1] == ()
    assert rewrite_spec("anything", {}) == ("anything", ())


def test_every_named_object_is_listed_once_and_a_spec_must_be_an_object() -> None:
    assert referenced_names(
        json.dumps({**_SPEC, "tool_resources": {**_SPEC["tool_resources"], "x": {"semantic_view": "DB.SV.SALES"}}})
    ) == (
        "db.sv.sales",
        "db.t.search",
        "db.t.lookup",
        "db.a.helper",
        "db.sk.s",
        "db.sk.t",
    )
    assert referenced_names("{}") == ()
    with pytest.raises(ValueError, match="JSON object"):
        rewrite_spec("[]", {"a": Rename("b")})
    with pytest.raises(ValueError, match="JSON object"):
        referenced_names("[]")


def test_what_changed_in_a_spec_is_named_per_instruction_and_per_top_level_key() -> None:
    before = {"instructions": {"response": "Be brief.", "system": "S"}, "models": {"orchestration": "a"}}
    after = {"instructions": {"response": "Be long.", "system": "S", "orchestration": "O"}, "tools": []}
    assert spec_changes(json.dumps(before), json.dumps(after)) == (
        "instructions.orchestration",
        "instructions.response",
        "models",
        "tools",
    )
    assert spec_changes(json.dumps(before), json.dumps(before)) == ()
    # Instructions added or removed whole are named by each instruction they hold.
    assert spec_changes("{}", json.dumps({"instructions": {"response": "R"}})) == ("instructions.response",)
    assert spec_changes(json.dumps({"instructions": "odd"}), "{}") == ("instructions",)
    with pytest.raises(ValueError, match="JSON object"):
        spec_changes("[]", "{}")


# A recorded stream's shapes, every event type once, an unknown one and a split payload included.
_STREAM = (Path(__file__).parents[2] / "fixtures" / "ask" / "stream.sse").read_text(encoding="utf-8")


def _decoded(text: str) -> list[AgentEvent]:
    decoder = SseDecoder()
    found = [decode(event) for line in text.split("\n") if (event := decoder.feed(line)) is not None]
    last = decoder.feed("")
    return found + ([decode(last)] if last else [])


def test_a_stream_decodes_into_typed_events_and_an_unknown_one_is_kept_to_ignore() -> None:
    events = _decoded(_STREAM)
    assert events[0] == MessageIds("user", 11, "5-11", 0)
    assert events[1] == Status("planning", "Planning the next steps", 1)
    assert events[2] == ThinkingDelta("Thinking", 2)
    assert isinstance(events[3], ToolUse) and (events[3].name, dict(events[3].input)) == ("SALES", {"query": "x"})
    assert events[4] == ToolProgress("t1", "executing_tool", "Running SALES", 4)
    result = events[5]
    assert isinstance(result, ToolResult)
    assert (result.sql, result.semantic_model, result.table) == (
        "SELECT 1",
        "SCR.X.SALES__SST_ASK_1",
        ResultTable(("ORDER_COUNT",), (("61948",),)),
    )
    # A payload split over two `data:` lines is joined before it is parsed.
    assert events[6:8] == [TextDelta("There are ", 6), TextDelta("61,948 orders.", 7)]
    assert events[8] == Chart('{"mark":"bar"}', 8)
    assert events[9] == Warning("a tool was skipped", "399569", 9)
    assert events[10] == Suggestions(("And by month?",))
    assert events[11] == MessageIds("assistant", 12, "", 11)
    assert events[12] == Unknown("response.something_new")
    assert events[13] == Finished("completed", "5-11", 12, 107, 13)
    assert events[14] == Done()


def test_an_error_event_odd_payloads_and_a_stream_without_a_final_blank_line_decode() -> None:
    assert decode(SseEvent("error", {"code": "390112", "message": "expired", "request_id": "r"})) == RunError(
        "expired", "390112", "r"
    )
    assert decode(SseEvent("response.text.delta", "not json")) == TextDelta("", None)
    assert decode(SseEvent("response.tool_use", {"input": "odd", "name": 3})) == ToolUse("", "3", "", sequence=None)
    assert decode(SseEvent("response.tool_result", {"content": ["odd"]})).table is None  # type: ignore[union-attr]
    assert decode(SseEvent("metadata", {"metadata": "odd"})) == MessageIds("", 0, "", None)
    assert decode(SseEvent("response", {"status": "timed_out", "metadata": "odd"})) == Finished("timed_out")
    assert decode(SseEvent("response.suggested_queries", {"suggested_queries": "odd"})) == Suggestions(())
    assert _decoded("event: done\ndata: [DONE]") == [Done()]
    assert SseDecoder().feed("") is None and SseDecoder().feed("id: 7") is None
    assert result_table("odd") is None and result_table({"resultSetMetaData": {"rowType": "odd"}}) is None
    assert result_table({"resultSetMetaData": {"rowType": ["odd"]}, "data": ["x"]}) == ResultTable(("",), (("x",),))


def test_a_turn_adds_up_its_events_and_the_thread_follows_its_reply() -> None:
    turn = Turn()
    for event in _decoded(_STREAM):
        turn = add(turn, event)
    assert turn.answer == "There are 61,948 orders." and turn.thinking == "Thinking"
    assert (len(turn.tool_uses), len(turn.tool_results), len(turn.warnings)) == (1, 1, 1)
    assert (turn.status, turn.run_id, turn.assistant_message_id, turn.last_sequence, turn.tokens) == (
        "completed",
        "5-11",
        12,
        13,
        107,
    )
    assert turn.finished
    thread = Thread(5).after(turn)
    assert (thread.parent, thread.turns, thread.tokens) == (12, 1, 107)
    # A turn with no reply id leaves the thread on the last reply that had one.
    assert Thread(5, parent=9).after(Turn(status="cancelled")).parent == 9
    failed = add(Turn(), RunError("refused", "399569"))
    assert failed.finished and failed.error == RunError("refused", "399569")
    assert not Turn().finished
    assert run_id(5, 11) == "5-11"


def test_a_reference_to_the_object_is_renamed_and_nothing_else_is() -> None:
    old, new = QualifiedName.parse("DB.S.F"), QualifiedName.parse("SCR.X.F__SST_ASK_AB12CD34")
    statement = sql(
        "CREATE OR REPLACE FUNCTION {name}(A NUMBER) COMMENT = {comment} AS $$ DB.S.F $$",
        name=qname(old),
        comment=literal("calls DB.S.F"),
    )
    assert renamed(statement, old, new).text == (
        "CREATE OR REPLACE FUNCTION SCR.X.F__SST_ASK_AB12CD34(A NUMBER) COMMENT = 'calls DB.S.F' AS $$ DB.S.F $$"
    )
    assert renamed(sql('SELECT * FROM X.DB.S.F, db.s.f, "DB"."S"."F", DB.S.F.G, DB.S, DB. S'), old, new).text == (
        "SELECT * FROM X.DB.S.F, SCR.X.F__SST_ASK_AB12CD34, SCR.X.F__SST_ASK_AB12CD34, DB.S.F.G, DB.S, DB. S"
    )


def test_an_orchestration_that_is_not_an_object_is_left_as_it_is() -> None:
    text, done = rewrite_spec(json.dumps({**_SPEC, "orchestration": "odd"}), {"db.sv.sales": Rename("A.B.C")})
    assert json.loads(text)["orchestration"] == "odd" and done == ("db.sv.sales",)
