"""How a suite schedules its attempts: retries only after a non-pass, parallel baseline rounds.

The runs are doubles on sessions of a real `ConnectorPool`. Every wait between two status reads
goes to a `RoundClock`, which ends a round only once every run that can be in flight is waiting,
so a round is one tick of a logical clock and no test waits on the wall clock.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunOptions, EvalRunsInterrupted, EvalSuiteResult, RunEvalSuite
from snowflake_semantic_tools.domain.model.eval import EvalDefaults
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import STATUS_COLUMNS, compiled_eval_of, result_rows
from tests.helpers.snowflake_fake import FakeSnowflake

BASE = "EVAL_{agent}_abcdef0_ci_20260928T010203Z"
_RUN_NAME = re.compile(r"'run_name', '([^']+)'")


class RoundClock(FixedClock):
    """Ends a round once `min(cap, runs still to finish)` runs wait between two status reads.

    A run's first status read is in progress and its next, a round later, terminal. Every
    hand-off has a 5-second safety bound that a passing test never reaches.
    """

    def __init__(self, total: int, cap: int, outcomes: dict[str, str] | None = None) -> None:
        super().__init__()
        self.total = total
        self.cap = cap
        self.outcomes = outcomes or {}
        self.tick = 0
        self.in_flight = 0
        self.most_in_flight = 0
        self.started_at: dict[str, int] = {}
        self.sessions: dict[str, set[int]] = {}
        self._finished = 0
        self._waiting = 0
        self._condition = threading.Condition()

    def start(self, run_name: str, session: object) -> None:
        with self._condition:
            self.in_flight += 1
            self.most_in_flight = max(self.most_in_flight, self.in_flight)
            self.started_at[run_name] = self.tick
            self.sessions.setdefault(run_name, set()).add(id(session))

    def status(self, run_name: str, session: object) -> str:
        with self._condition:
            self.sessions[run_name].add(id(session))
            if self.tick == self.started_at[run_name]:
                return "CREATED"
            self.in_flight -= 1
            self._finished += 1
            self._advance_if_full()
            return self.outcomes.get(run_name, "COMPLETED")

    def sleep(self, milliseconds: int) -> None:
        del milliseconds
        with self._condition:
            self._waiting += 1
            tick = self.tick
            self._advance_if_full()
            if self.tick == tick and not self._condition.wait_for(lambda: self.tick != tick, timeout=5):
                raise AssertionError(f"round {tick} never filled: {self._waiting} run(s) waited")

    def _advance_if_full(self) -> None:
        """End the round once every run that can be in flight waits; the caller holds the lock."""
        if self._waiting and self._waiting == min(self.cap, self.total - self._finished):
            self.tick += 1
            self._waiting = 0
            self._condition.notify_all()


class RoundSession(FakeSnowflake):
    """Starts, reports, and reads each run through the clock, by the name its call carries."""

    def __init__(self, clock: RoundClock, agents: tuple[str, ...]) -> None:
        super().__init__()
        self.clock = clock
        self.agents = agents
        for agent in agents:
            self.agent_versions[(f"DB.S.{agent.upper()}", "committed")] = "VERSION$1"

    def close(self) -> None:
        pass

    def query_in_context(self, scope: SchemaScope, sql: Sql, params: object = None) -> QueryResult:
        del scope
        text = str(sql)
        if "EXECUTE_AI_EVALUATION('START'" in text:
            match = _RUN_NAME.search(text)
            assert match is not None
            self.clock.start(match.group(1), self)
            return QueryResult()
        assert isinstance(params, tuple)
        run_name = next(str(value) for value in params if str(value).startswith("EVAL_"))
        if "STATUS" in text:
            status = self.clock.status(run_name, self)
            agent = next(name.upper() for name in self.agents if run_name.startswith(f"EVAL_{name.upper()}_"))
            return QueryResult(STATUS_COLUMNS, ((run_name, agent, "CORTEX AGENT", status, []),))
        return result_rows()


def _evals(
    agents: tuple[str, ...], *, baseline_runs: int | None = None, retry: int | None = None
) -> tuple[CompiledEval, ...]:
    base = compiled_eval_of()
    config_run = base.resolved.config.run
    assert config_run is not None
    run = replace(config_run, baseline_runs=baseline_runs, retry=retry)
    return tuple(
        replace(
            base,
            resolved=replace(
                base.resolved,
                agent=replace(base.resolved.agent, name=agent),
                config=replace(base.resolved.config, run=run),
            ),
            agent_target=QualifiedName.parse(f"DB.S.{agent.upper()}"),
        )
        for agent in agents
    )


def _run(
    clock: RoundClock,
    agents: tuple[str, ...],
    *,
    baseline_capture: bool,
    baseline_runs: int | None = None,
    retry: int | None = None,
) -> tuple[EvalSuiteResult, RoundSession]:
    first = RoundSession(clock, agents)
    with ConnectorPool(clock.cap, lambda: RoundSession(clock, agents)) as pool:
        suite = RunEvalSuite(first, clock, sessions=pool).run(
            _evals(agents, baseline_runs=baseline_runs, retry=retry),
            defaults=EvalDefaults(concurrency=clock.cap),
            options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
            baseline_capture=baseline_capture,
        )
    return suite, first


def _names(agent: str, count: int) -> list[str]:
    base = BASE.format(agent=agent.upper())
    return [base, *(f"{base}_R{number}" for number in range(2, count + 1))]


def test_a_baseline_starts_its_runs_in_parallel_within_the_concurrency_in_attempt_order() -> None:
    clock = RoundClock(total=5, cap=2)

    suite, first = _run(clock, ("sales_agent",), baseline_capture=True, baseline_runs=5, retry=1)

    attempts = suite.evals[0].attempts
    assert suite.success and [attempt.attempt for attempt in attempts] == [1, 2, 3, 4, 5]
    assert [attempt.run_name for attempt in attempts] == _names("sales_agent", 5)
    assert clock.most_in_flight == 2 and clock.tick == 3  # ceil(5 / 2) rounds
    assert sorted(clock.started_at.values()) == [0, 0, 1, 1, 2]
    # Each run starts and polls on one session of its own, never the command's.
    assert all(len(sessions) == 1 and id(first) not in sessions for sessions in clock.sessions.values())


def test_baselines_of_several_evals_share_one_concurrency_budget() -> None:
    agents = ("alpha", "beta", "gamma")
    clock = RoundClock(total=6, cap=2)

    suite, _ = _run(clock, agents, baseline_capture=True, baseline_runs=2, retry=0)

    assert [[attempt.run_name for attempt in result.attempts] for result in suite.evals] == [
        _names(agent, 2) for agent in agents
    ]
    assert clock.most_in_flight == 2 and clock.tick == 3


def test_a_baseline_retries_only_its_attempts_that_did_not_pass() -> None:
    names = _names("sales_agent", 4)
    clock = RoundClock(total=4, cap=3, outcomes={names[1]: "FAILED"})

    suite, _ = _run(clock, ("sales_agent",), baseline_capture=True, baseline_runs=3, retry=2)

    attempts = suite.evals[0].attempts
    assert [(attempt.run_name, attempt.terminal_status) for attempt in attempts] == [
        (names[0], "COMPLETED"),
        (names[1], "FAILED"),
        (names[2], "COMPLETED"),
        (names[3], "COMPLETED"),
    ]
    # The first round showed which retry was needed, so it is a round of its own.
    assert clock.started_at[names[3]] == 1 and suite.evals[0].accepted


def test_a_gate_run_starts_one_attempt_per_eval_and_a_retry_only_after_a_failure() -> None:
    agents = ("alpha", "beta")
    clock = RoundClock(total=3, cap=2, outcomes={_names("beta", 1)[0]: "FAILED"})

    suite, _ = _run(clock, agents, baseline_capture=False, retry=1)

    assert [[attempt.terminal_status for attempt in result.attempts] for result in suite.evals] == [
        ["COMPLETED"],
        ["FAILED", "COMPLETED"],
    ]
    assert clock.started_at == {_names("alpha", 1)[0]: 0, _names("beta", 1)[0]: 0, _names("beta", 2)[1]: 1}


class RecordingPool:
    """Lends a fresh session per lease and records each halt."""

    def __init__(self, session: Callable[[], FakeSnowflake]) -> None:
        self.session = session
        self.halts: list[str] = []

    @contextmanager
    def lease(self) -> Iterator[FakeSnowflake]:
        yield self.session()

    def halt(self, reason: str) -> None:
        self.halts.append(reason)


def test_an_interrupt_names_every_run_it_started_and_halts_every_session() -> None:
    clock = RoundClock(total=2, cap=2)

    class Interrupted(RoundSession):
        def __init__(self) -> None:
            super().__init__(clock, ("sales_agent",))

        def query_in_context(self, scope: SchemaScope, sql: Sql, params: object = None) -> QueryResult:
            reply = super().query_in_context(scope, sql, params)
            if "'START'" in str(sql) and len(clock.started_at) == 2:
                raise KeyboardInterrupt
            return reply

    pool = RecordingPool(Interrupted)
    use_case = RunEvalSuite(Interrupted(), FixedClock(), sessions=pool)

    with pytest.raises(EvalRunsInterrupted) as raised:
        use_case.run(
            _evals(("sales_agent",), baseline_runs=2, retry=0),
            defaults=EvalDefaults(concurrency=2),
            options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z"),
            baseline_capture=True,
        )

    assert raised.value.run_names == tuple(_names("sales_agent", 2))
    assert pool.halts == ["the eval run was interrupted"]
