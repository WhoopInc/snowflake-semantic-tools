"""The `sst import` wizard: each question's default, retries on an answer it cannot read, and its listing."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from snowflake_semantic_tools.cli.import_wizard import Answers, Wizard
from tests.helpers.import_fakes import TARGET, FakeImportPort, staged_agent


def _answering(monkeypatch: pytest.MonkeyPatch, *answers: str) -> list[str]:
    asked: list[str] = []
    replies: Iterator[str] = iter(answers)

    def ask(question: str, default: str) -> str:
        asked.append(question)
        return next(replies)

    monkeypatch.setattr("snowflake_semantic_tools.cli.confirm.ask", ask)
    return asked


def test_a_scope_given_asks_only_for_the_type(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _answering(monkeypatch, "semantic_view")
    answers = Wizard(FakeImportPort(), TARGET).ask(None, "DB.S", with_dependencies=True, to_folder="team")
    assert answers == Answers("semantic_view", "DB.S", (), True, "team")
    assert asked == ["Type to import"]


def test_a_choice_it_cannot_read_is_asked_again_and_a_number_picks_by_position(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _answering(monkeypatch, "views", "9", "1")
    answers = Wizard(FakeImportPort(), TARGET).ask(None, "DB.S", with_dependencies=False, to_folder=None)
    assert answers.artifact_type == "semantic_view"
    assert capsys.readouterr().err.count("choose 1-7, or one of:") == 2


def test_a_whole_database_lists_every_schema_and_keeps_the_flags_given(monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.ONE")
    staged_agent(port, "DB.T.TWO")
    asked = _answering(monkeypatch, "database", "DB", "all")
    answers = Wizard(port, TARGET).ask("agent", None, with_dependencies=True, to_folder="team")
    assert answers == Answers("agent", "DB", (), True, "team")
    assert asked == [
        "Import one object, a schema, or a whole database?",
        "Database",
        "Objects to import (numbers such as 1,3-5, or all)",
    ]


def test_a_name_that_is_not_an_identifier_is_asked_again(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _answering(monkeypatch, "database", "not valid", "DB", "", "n")
    answers = Wizard(FakeImportPort(), TARGET).ask("agent", None, with_dependencies=False, to_folder=None)
    assert answers == Answers("agent", "DB", (), False, None)
    assert "'not valid' is not a Snowflake identifier" in capsys.readouterr().err


def test_a_listing_that_is_refused_leaves_the_run_to_report_it(monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeImportPort()
    port.refuse("show_objects_in")
    asked = _answering(monkeypatch, "schema", "DB", "S", "", "y")
    answers = Wizard(port, TARGET).ask("agent", None, with_dependencies=False, to_folder=None)
    assert answers == Answers("agent", "DB.S", (), True, None)
    assert "Objects to import (numbers such as 1,3-5, or all)" not in asked


def test_a_pick_out_of_range_is_asked_again_and_one_object_picks_the_first_by_default(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.ONE")
    staged_agent(port, "DB.S.TWO")
    _answering(monkeypatch, "object", "DB", "S", "5", "all", "", "n")
    answers = Wizard(port, TARGET).ask("agent", None, with_dependencies=False, to_folder=None)
    # Picking every object for one object's run still names them, so the run reads only those.
    assert answers.only == ("ONE", "TWO")
    assert "choose numbers from 1 to 2, ranges such as 2-4, or all" in capsys.readouterr().err
