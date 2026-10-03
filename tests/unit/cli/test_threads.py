"""`--threads` on `plan`, `apply` and `test`: how it resolves, and that output never depends on it.

Each run here opens its sessions from a double that answers slowly at random, so concurrent
reads finish out of order, and that refuses some reads and statements, so the diagnostics'
order is exercised as well as the plan's.
"""

from __future__ import annotations

import json
import random
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.settings import apply_parallelism, threads_setting
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.cli_projects import common, compile_project, project_copy
from tests.helpers.projects import project_paths


class JitteredSnowflake:
    """Open recorded sessions that answer after a random pause, and track which are open."""

    def __init__(self) -> None:
        self.opened: list[_Session] = []
        self.threads: set[str] = set()
        self._guard = threading.Lock()

    def open(self, params: object) -> _Session:
        del params
        session = _Session(self)
        with self._guard:
            self.opened.append(session)
        return session

    def saw(self) -> None:
        with self._guard:
            self.threads.add(threading.current_thread().name)

    @property
    def still_open(self) -> int:
        return sum(not session.closed for session in self.opened)


def _pause(owner: JitteredSnowflake) -> None:
    owner.saw()
    time.sleep(random.uniform(0, 0.004))


class _Session:
    """One recorded session: refuses the agent listing, the lock reads, and one metric's EXPLAIN."""

    def __init__(self, owner: JitteredSnowflake) -> None:
        # Imported here: the recorded double's module is large, and only these tests subclass it.
        from tests.helpers.recorded_snowflake import RecordedSnowflake

        self._owner = owner
        self._recorded = RecordedSnowflake()
        self._recorded.preflight.refused = {"locked_objects"}
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        _pause(self._owner)
        if object_type.upper() == "AGENT":
            raise SnowflakePortError(f"listing agents in {scope.sql} refused")
        return self._recorded.show_objects(object_type, scope)

    def query(self, sql: Sql, params: Any = None) -> QueryResult:
        _pause(self._owner)
        if "EXPLAIN" in str(sql) and "AVG" in str(sql).upper():
            raise SnowflakePortError("recorded EXPLAIN refusal")
        return self._recorded.query(sql, params)

    def query_in_context(self, scope: SchemaScope, sql: Sql, params: Any = None) -> QueryResult:
        del scope
        return self.query(sql, params)

    def schema_exists(self, scope: SchemaScope) -> bool:
        _pause(self._owner)
        return bool(self._recorded.schema_exists(scope))

    def relation_exists(self, qualified_name: QualifiedName) -> bool:
        _pause(self._owner)
        return bool(self._recorded.relation_exists(qualified_name))

    def database_exists(self, database: Identifier) -> bool:
        _pause(self._owner)
        return bool(self._recorded.database_exists(database))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._recorded, name)


def _plan(
    monkeypatch: pytest.MonkeyPatch, project: Path, *extra: str, env: dict[str, str] | None = None
) -> tuple[dict[str, Any], JitteredSnowflake]:
    sessions = JitteredSnowflake()
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", sessions.open)
    args = ["plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out", *extra]
    result = CliRunner().invoke(cli, [*args, "--snowflake-syntax-check", "--partial"], env=env)
    assert result.exit_code in (1, 2), result.output
    envelope = json.loads(result.output)
    return envelope, sessions


def _comparable(envelope: dict[str, Any]) -> tuple[object, ...]:
    """What a plan reports, less what names the run rather than the plan: its id and when it observed."""
    data = envelope["data"]
    return (envelope["exit_code"], data["changes"], data["report_only"], data["partial"], envelope["diagnostics"])


@pytest.fixture
def project(tmp_path: Path) -> Path:
    copy = project_copy(tmp_path)
    compile_project(copy)
    return copy


def test_plan_reports_the_same_on_one_thread_and_on_four(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    one, one_sessions = _plan(monkeypatch, project, "--threads", "1")
    four, four_sessions = _plan(monkeypatch, project, "--threads", "4")
    again, _ = _plan(monkeypatch, project, "--threads", "4")

    assert _comparable(one) == _comparable(four) == _comparable(again)
    refused = [item["message"] for item in one["diagnostics"] if item["code"] in ("SST-PLN001", "SST-VAL418")]
    # Validation, observation and preflight each reported refusals, so their order was compared too.
    assert any("AVG_ORDER_VALUE" in message for message in refused)
    assert any(message.startswith("observation of AGENT in") for message in refused)
    assert any("locks in" in message for message in refused)
    assert len(one_sessions.opened) == 1 and one_sessions.threads == {"MainThread"}
    assert 1 < len(four_sessions.opened) <= 4
    assert any(name.startswith("sst-worker") for name in four_sessions.threads)
    assert four_sessions.still_open == 0 and one_sessions.still_open == 0


def test_plan_threads_come_from_the_environment_then_the_config(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    one, _ = _plan(monkeypatch, project)
    from_env, env_sessions = _plan(monkeypatch, project, env={"SST_THREADS": "3"})
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8") + "generation:\n  threads: 2\n", encoding="utf-8")
    compile_project(project)
    from_config, config_sessions = _plan(monkeypatch, project)
    flag_wins, flag_sessions = _plan(monkeypatch, project, "--threads", "1", env={"SST_THREADS": "4"})

    assert _comparable(one) == _comparable(from_env)
    assert _comparable(from_config) == _comparable(flag_wins)
    assert 1 < len(env_sessions.opened) <= 3
    assert 1 < len(config_sessions.opened) <= 2
    assert len(flag_sessions.opened) == 1
    assert env_sessions.still_open == config_sessions.still_open == flag_sessions.still_open == 0


def test_plan_refuses_a_thread_count_outside_one_to_sixteen(project: Path) -> None:
    for value in ("0", "17"):
        result = CliRunner().invoke(cli, ["plan", *common(project), "--threads", value])
        assert result.exit_code == 3, result.output


def test_test_takes_threads_from_the_flag_or_the_environment_within_one_to_sixteen(project: Path) -> None:
    golden = ["test", *common(project), "--suite", "golden", "--output", "json"]
    for args, env in ((["--threads", "4"], None), ([], {"SST_THREADS": "4"})):
        result = CliRunner().invoke(cli, [*golden, *args], env=env)
        assert json.loads(result.output)["command"] == "test" and result.exit_code in (0, 1, 4), result.output
    # A bad flag is a usage error; a bad environment value is a configuration error.
    assert CliRunner().invoke(cli, [*golden, "--threads", "17"]).exit_code == 3
    refused = CliRunner().invoke(cli, golden, env={"SST_THREADS": "0"})
    assert refused.exit_code == 4
    assert [item["code"] for item in json.loads(refused.output)["diagnostics"]] == ["SST-CFG004"]


def test_threads_resolve_from_the_flag_then_generation_threads(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("validation:\n  snowflake_syntax_check: false\n", encoding="utf-8")
    paths = project_paths(tmp_path)
    assert (threads_setting(paths, None), apply_parallelism(paths)) == (1, 4)
    assert (threads_setting(paths, 3), apply_parallelism(paths, 3)) == (3, 3)
    (tmp_path / "sst_config.yml").write_text(
        "validation:\n  snowflake_syntax_check: false\nskills:\n  +threads: 6\n", encoding="utf-8"
    )
    assert (threads_setting(paths, None), apply_parallelism(paths)) == (1, 6)
    (tmp_path / "sst_config.yml").write_text(
        "validation:\n  snowflake_syntax_check: false\ngeneration:\n  threads: 5\nskills:\n  +threads: 6\n",
        encoding="utf-8",
    )
    assert (threads_setting(paths, None), apply_parallelism(paths)) == (5, 5)
    assert (threads_setting(paths, 2), apply_parallelism(paths, 2)) == (2, 2)
