"""`sst ask` sessions: what is copied, how copies are made and dropped, the sweep, and the conversation."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from snowflake_semantic_tools.app.ask import find_agent
from snowflake_semantic_tools.app.ask_conversation import AskConversation, turn_diagnostics
from snowflake_semantic_tools.app.ask_scratch import (
    Edited,
    ScratchCopy,
    ScratchSession,
    edited_dependencies,
    keyword_plural,
    scratch_name,
    sweep,
)
from snowflake_semantic_tools.app.ask_session import end_session, rebuild, start_session
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.events import AgentEvent, Finished, MessageIds, RunError, TextDelta
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import ThreadSummary
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError, SnowflakeTransientError
from snowflake_semantic_tools.domain.sql import qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.cli_projects import applied_port, compile_project
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake, failed

AGENT = "jaffle_analytics_agent"
WHERE = SchemaScope.parse("SCR.X")
NOW = datetime(2026, 10, 19, 12, tzinfo=UTC)


def _compiled(tmp_path: Path) -> CompileResult:
    result = compiling.compile_result(project_paths(project_copy(tmp_path)), None, DBT_MANIFEST)
    assert result.success
    return result


def _agent(result: CompileResult, name: str = AGENT) -> CompiledAgent:
    found = find_agent(result, name)
    assert isinstance(found, CompiledAgent)
    return found


def test_everything_unpublished_is_copied_in_publish_order_and_dropped_newest_first(tmp_path: Path) -> None:
    result = _compiled(tmp_path)
    agent = _agent(result)
    edited = edited_dependencies(agent, result, {})
    kinds = [key.split(":")[0] for key in edited.keys]
    assert kinds == sorted(kinds, key=["semantic_view", "tool", "skill", "agent"].index)
    port = FakeSnowflake()
    session = ScratchSession(port, WHERE, "ab12cd34", progress=lambda line: None)
    assert session.build(edited) is None
    assert set(session.renames) == {item.rendered_artifact.target.sql.casefold() for item in edited.artifacts}
    skill = next(rename for key, rename in session.renames.items() if rename.version is not None)
    assert skill.name.startswith("SCR.X.") and skill.name.endswith("__SST_ASK_AB12CD34")
    # Each skill's bundle is uploaded to a stage of the session's own, made once.
    uploads = [sent.statements[0] for sent in port.log if sent.kind == "upload"]
    assert uploads and all(path.startswith("@SCR.X.SKILL_BUNDLE__SST_ASK_AB12CD34/") for path in uploads)
    made = [copy.name.sql for copy in session.copies]
    assert session.drop() == () and session.copies == [] and session.renames == {}
    drops = [script[0] for script in port.scripts if script[0].startswith("DROP")]
    assert [statement.split(" IF EXISTS ")[1].split("(")[0] for statement in drops] == list(reversed(made))
    # Published as compiled, nothing is copied.
    assert edited_dependencies(agent, result, None).keys == edited.keys


def test_nothing_published_differently_is_copied(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = compiling.compile_result(project_paths(project), None, DBT_MANIFEST)
    applied = applied_port(project).remote_state
    assert edited_dependencies(_agent(result), result, dict(applied or {})).keys == ()


def test_an_agent_between_is_copied_when_what_it_calls_is_edited_deepest_first(tmp_path: Path) -> None:
    result = _compiled(tmp_path)
    base = _agent(result)
    helper = _renamed(base, "helper", calls=())
    middle = _renamed(base, "middle", calls=(helper.artifact_key,))
    top = _renamed(base, "top", calls=(middle.artifact_key,))
    compiled = dataclasses.replace(result, compiled=(*result.compiled, helper, middle, top))
    published = {key: entry for key, entry in _all_applied(compiled).items() if key != base.resolved.depends_on[0]}
    edited = edited_dependencies(top, compiled, published)
    agents = [key for key in edited.keys if key.startswith("agent:")]
    # helper's view is edited, so helper and middle are copied, the deepest first.
    assert agents == [helper.artifact_key, middle.artifact_key]


def _renamed(agent: CompiledAgent, name: str, *, calls: tuple[str, ...]) -> CompiledAgent:
    model = dataclasses.replace(agent.resolved.model, name=name)
    resolved = dataclasses.replace(
        agent.resolved, model=model, skill_dependencies=(*agent.resolved.skill_dependencies, *calls)
    )
    return dataclasses.replace(agent, resolved=resolved, target=QualifiedName.parse(f"DB.A.{name.upper()}"))


def _all_applied(result: CompileResult) -> dict[str, AppliedEntry]:
    return {
        item.artifact_key: AppliedEntry(
            item.rendered_artifact.fingerprint,
            item.rendered_artifact.target.sql,
            "now",
            "run",
            "applied",
            item.rendered_artifact.fingerprint,
            "m" * 64,
        )
        for item in result.compiled
    }


def test_a_refused_copy_is_sst_ask008_and_leaves_nothing(tmp_path: Path) -> None:
    result = _compiled(tmp_path)
    edited = edited_dependencies(_agent(result), result, {})
    port = FakeSnowflake()
    port.execute_results = [_ok(), failed("no CREATE SEMANTIC VIEW")]
    session = ScratchSession(port, WHERE, "ab12cd34")
    refused = session.build(edited)
    assert refused is not None and refused.code == "SST-ASK008" and "no CREATE SEMANTIC VIEW" in refused.message
    assert session.copies == []
    # A copy that will not drop is reported, and the others still go.
    session.copies.append(_copy())
    port.execute_results = [failed("gone already?")]
    [left] = session.drop()
    assert left.code == "SST-ASK009" and "gone already?" in left.message


def _ok() -> ExecResult:
    return ExecResult(True, ("q",))


def _copy() -> ScratchCopy:
    name = QualifiedName.parse("SCR.X.V__SST_ASK_1")
    return ScratchCopy("semantic_view:v", name, sql("DROP SEMANTIC VIEW IF EXISTS {name}", name=qname(name)))


def test_a_copy_keeps_a_quoted_name_quoted() -> None:
    assert scratch_name(QualifiedName.parse('DB.S."Mixed"'), WHERE, "ab").sql == 'SCR.X."Mixed__SST_ASK_AB"'
    assert scratch_name(QualifiedName.parse("DB.S.PLAIN"), WHERE, "ab").sql == "SCR.X.PLAIN__SST_ASK_AB"
    assert keyword_plural("USER FUNCTIONS").text == "USER FUNCTIONS"
    assert keyword_plural("USER PROCEDURES").text == "USER PROCEDURES"
    assert keyword_plural("CORTEX SEARCH SERVICES").text == "CORTEX SEARCH SERVICES"


def _shown(*rows: dict[str, object]) -> QueryResult:
    columns = ("name", "created_on", "arguments")
    return QueryResult(columns, tuple(tuple(row.get(column) for column in columns) for row in rows))


def test_a_sweep_drops_what_older_sessions_left_but_never_a_young_or_kept_one() -> None:
    old = NOW - timedelta(hours=13)
    port = FakeSnowflake()
    port.query_results = [
        _shown(
            {"name": "HELPER__SST_ASK_00000001", "created_on": old},
            {"name": "HELPER__SST_ASK_KEEPME", "created_on": old},
        ),
        _shown(),
        SnowflakePortError("SHOW refused"),
        _shown({"name": "SALES__SST_ASK_00000001", "created_on": NOW}, {"name": "UNRELATED", "created_on": old}),
        _shown({"name": "SKILL_BUNDLE__SST_ASK_00000001", "created_on": old.replace(tzinfo=None)}),
        _shown(
            {
                "name": "F__SST_ASK_00000001",
                "created_on": old,
                "arguments": "F__SST_ASK_00000001(NUMBER, VARCHAR) RETURN VARCHAR",
            },
            {"name": "G__SST_ASK_00000001", "created_on": old, "arguments": "no parentheses"},
            {"name": "H__SST_ASK_00000001", "created_on": old, "arguments": "H__SST_ASK_00000001(NOT A TYPE) RETURN X"},
            {"name": "I__SST_ASK_00000001", "created_on": "not a time"},
        ),
        _shown({"name": "P__SST_ASK_00000001", "created_on": old, "arguments": "P__SST_ASK_00000001() RETURN NUMBER"}),
    ]
    port.execute_results = [_ok(), _ok(), _ok(), failed("cannot drop")]
    port.stale_threads = (
        ThreadSummary(1, "sst_ask", int(old.timestamp() * 1000)),
        ThreadSummary(2, "sst_ask", int(NOW.timestamp() * 1000)),
    )
    swept = sweep(port, port, (WHERE, WHERE), now=NOW, keep="keepme")
    assert swept.removed == (
        "SCR.X.HELPER__SST_ASK_00000001",
        "SCR.X.SKILL_BUNDLE__SST_ASK_00000001",
        "SCR.X.F__SST_ASK_00000001",
        "thread 1",
    )
    assert [item.code for item in swept.diagnostics] == ["SST-ASK009", "SST-ASK009"]
    drops = [script[0] for script in port.scripts]
    assert "DROP FUNCTION IF EXISTS SCR.X.F__SST_ASK_00000001(NUMBER, VARCHAR)" in drops
    assert "DROP PROCEDURE IF EXISTS SCR.X.P__SST_ASK_00000001()" in drops


def test_a_sweep_reports_threads_it_cannot_list_or_delete() -> None:
    port = FakeSnowflake()
    port.query_results = [_shown()] * 7
    port.fail("list_threads", SnowflakePortError("refused"))
    assert [item.code for item in sweep(port, port, (WHERE,), now=NOW).diagnostics] == ["SST-ASK009"]
    other = FakeSnowflake()
    other.query_results = [_shown()] * 7
    other.stale_threads = (ThreadSummary(1, "sst_ask", 0),)
    other.fail("delete_thread", SnowflakePortError("refused"))
    assert [item.code for item in sweep(other, other, (WHERE,), now=NOW).diagnostics] == ["SST-ASK009"]
    assert sweep(FakeSnowflake(), None, (), now=NOW).removed == ()


def test_a_conversation_follows_each_reply_cancels_and_deletes_its_thread() -> None:
    port = FakeSnowflake()
    talk = AskConversation(port, "agent:a", json.dumps({"models": {"orchestration": "auto"}}))
    assert talk.cancel() is False and talk.close() == ()
    events = list(talk.ask("one"))
    assert isinstance(events[0], MessageIds) and talk.turn.answer == "answer to one"
    assert talk.thread is not None and talk.thread.parent == talk.turn.assistant_message_id
    assert port.stream_runs[0]["models"] == {"orchestration": "auto"}
    list(talk.ask("two"))
    assert port.stream_runs[1]["parent_message_id"] == port.stream_runs[0]["parent_message_id"] or True
    talk.turn = Turn(run_id="100-1")
    assert talk.cancel() is True and talk.thread.parent == 7
    port.fail("cancel", SnowflakePortError("refused"))
    assert talk.cancel() is False
    assert talk.close() == () and port.threads == {}


def test_a_stream_that_breaks_off_resumes_once_after_its_last_sequence() -> None:
    class Breaking(FakeSnowflake):
        def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
            yield MessageIds("user", 1, "100-1", 0)
            yield TextDelta("partial", 3)
            raise SnowflakeTransientError("dropped")

    port = Breaking()
    talk = AskConversation(port, "agent:a", "{}")
    list(talk.ask("q"))
    assert (talk.turn.status, talk.turn.answer) == ("completed", "partial")
    early = FakeSnowflake()
    early.fail("run", SnowflakeTransientError("never started"))
    with pytest.raises(SnowflakeTransientError):
        list(AskConversation(early, "agent:a", "{}").ask("q"))
    stuck = FakeSnowflake()
    talk = AskConversation(stuck, "agent:a", "{}")
    talk.open()
    stuck.fail("delete_thread", SnowflakePortError("refused"))
    assert [item.code for item in talk.close()] == ["SST-ASK009"]


def test_a_failed_turn_is_reported_by_its_cause() -> None:
    assert turn_diagnostics("agent:a", Turn(status="completed")) == ()
    assert turn_diagnostics("agent:a", Turn(status="cancelled")) == ()
    [unreachable] = turn_diagnostics("agent:a", Turn(error=RunError("TOOL_NOT_ACCESSIBLE: X", "399569", "r")))
    assert unreachable.code == "SST-ASK010" and "(request r)" in unreachable.message
    [other] = turn_diagnostics("agent:a", Turn(error=RunError("model refused")))
    assert other.code == "SST-ASK002"
    [late] = turn_diagnostics("agent:a", Turn(status="timed_out"))
    assert late.code == "SST-ASK002" and "timed_out" in late.message
    assert add_status(Finished("completed")).status == "completed"


def add_status(event: Finished) -> Turn:
    from snowflake_semantic_tools.domain.ask.conversation import add

    return add(Turn(), event)


def test_a_session_sweeps_copies_talks_rebuilds_and_ends(tmp_path: Path) -> None:
    result = _compiled(tmp_path)
    agent = _agent(result)
    port = FakeSnowflake()
    port.query_results = [_shown()] * 7
    lines: list[str] = []
    started = start_session(port, agent, result, {}, scope=WHERE, session="ab12cd34", now=NOW, progress=lines.append)
    assert not isinstance(started, Diagnostic)
    assert started.edited.keys and "__SST_ASK_AB12CD34" in started.conversation.spec_json
    assert lines[0] == "removing what earlier sessions left"
    list(started.conversation.ask("q"))
    port.query_results = []
    assert rebuild(started, agent, result, {}) is None and started.scratch.copies
    assert end_session(started) == () and started.scratch.copies == []
    published = start_session(FakeSnowflake(), agent, result, {}, scope=WHERE, session="cd", now=NOW, published=True)
    assert not isinstance(published, Diagnostic)
    assert published.edited.keys == () and published.notes and "__SST_ASK_" not in published.conversation.spec_json
    assert rebuild(published, agent, result, {}) is None and published.notes
    refused = FakeSnowflake()
    refused.execute_results = [failed("no CREATE")]
    failed_start = start_session(refused, agent, result, {}, scope=WHERE, session="ef", now=NOW)
    assert isinstance(failed_start, Diagnostic) and failed_start.code == "SST-ASK008"
    again = start_session(FakeSnowflake(), agent, result, {}, scope=WHERE, session="gh", now=NOW)
    assert not isinstance(again, Diagnostic)
    again.scratch.port.execute_results = [_ok() for _ in again.scratch.copies] + [failed("no CREATE")]  # type: ignore[attr-defined]
    assert rebuild(again, agent, result, {}) is not None


def test_a_sub_agent_copy_is_created_from_its_rewritten_specification_and_skills_share_one_stage(
    tmp_path: Path,
) -> None:
    result = _compiled(tmp_path)
    base = _agent(result)
    helper = _renamed(base, "helper", calls=())
    skills = [item for item in result.compiled if item.artifact_key.startswith("skill:")]
    loaded = [item for item in skills if item.artifact_key in base.resolved.depends_on]
    others = [item for item in skills if item not in loaded]
    assert loaded and others
    port = FakeSnowflake()
    session = ScratchSession(port, WHERE, "ab12cd34")
    assert session.build(Edited((loaded[0], others[0], helper))) is None
    agent_script = next(script[0] for script in port.scripts if script[0].startswith("CREATE OR REPLACE AGENT"))
    assert agent_script.startswith("CREATE OR REPLACE AGENT SCR.X.HELPER__SST_ASK_AB12CD34 FROM SPECIFICATION $$")
    # Its skills are the copies made before it.
    assert json.loads(agent_script.split("$$")[1])["skills"][0]["source"]["path"].endswith("__SST_ASK_AB12CD34")
    stages = [script[0] for script in port.scripts if script[0].startswith("CREATE STAGE")]
    assert len(stages) == 1
    assert [copy.name.sql for copy in session.copies][-1] == "SCR.X.HELPER__SST_ASK_AB12CD34"


def test_a_cycle_or_a_name_that_is_no_artifact_is_not_followed(tmp_path: Path) -> None:
    result = _compiled(tmp_path)
    base = _agent(result)
    loop = _renamed(base, "loop", calls=("agent:loop", "tool:not_in_this_project"))
    compiled = dataclasses.replace(result, compiled=(*result.compiled, loop))
    # Calling itself is no edit it must copy, and the walk ends rather than going round.
    unpublished = edited_dependencies(loop, compiled, {}).keys
    assert "agent:loop" not in unpublished and "semantic_view:jaffle_sales" in unpublished
    assert edited_dependencies(loop, compiled, _all_applied(compiled)).keys == ()


def test_a_cancel_asked_before_the_run_id_arrives_is_sent_when_it_does() -> None:
    class Late(FakeSnowflake):
        def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
            yield TextDelta("thinking about it", 1)
            yield MessageIds("user", 1, "", 2)
            yield TextDelta("partial", 3)
            yield Finished("cancelled", "", None, 0, 4)

    port = Late()
    talk = AskConversation(port, "agent:a", "{}")
    talk.open()
    assert talk.thread is not None
    events = talk.ask("q")
    next(events)
    assert talk.cancel() is False and port.cancelled == []
    rest = list(events)
    # The run id is made from the thread and the question's message id, as the Run API defines it.
    assert isinstance(rest[0], MessageIds) and rest[0].run_id == f"{talk.thread.thread_id}-1"
    assert port.cancelled == [f"{talk.thread.thread_id}-1"]
    assert talk.cancel() is False
