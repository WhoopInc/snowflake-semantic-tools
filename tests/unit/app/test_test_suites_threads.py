"""The `test` suites report the same on one session as on four: smoke probes, markers, and evals.

The sessions come from a real `ConnectorPool` over doubles that answer after a random pause,
so concurrent work finishes out of order, and that refuse some of it, so the order of the
diagnostics is compared as well as the results.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunOptions, EvalSuiteResult, RunEvalSuite, eval_suite_json
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.smoke import SmokePublished, SmokeResult
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import AppliedEntry, Manifest
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore
from tests.helpers.compile_builders import compiled
from tests.helpers.eval_builders import STATUS_COLUMNS, compiled_eval_of, result_rows
from tests.helpers.project_inputs import EMPTY_SOURCES, dev_target

STATE_TABLE = QualifiedName.parse("DB.SCH.SST_STATE")
VIEWS = ("MENU", "SALES", "ORDERS", "STORES", "ITEMS", "SUPPLIES")


def _pause() -> None:
    time.sleep(random.uniform(0, 0.003))


class Opened:
    """The sessions a run opened, and the threads that used them."""

    def __init__(self) -> None:
        self.sessions: list[TrackedSession] = []
        self.threads: set[str] = set()
        self.guard = threading.Lock()

    def saw(self) -> None:
        with self.guard:
            self.threads.add(threading.current_thread().name)


class TrackedSession(InMemorySnowflake):
    """A session that registers itself with the run that opened it, and records its close."""

    def __init__(self, opened: Opened) -> None:
        super().__init__()
        self.opened = opened
        self.closed = False
        with opened.guard:
            opened.sessions.append(self)

    def close(self) -> None:
        self.closed = True


class SmokeSession(TrackedSession):
    """A published target: probes of SALES and STORES fail, and ITEMS carries another run's marker."""

    def __init__(self, opened: Opened, result: CompileResult, manifest: Manifest) -> None:
        super().__init__(opened)
        self.remote_state = MappingProxyType(
            {
                artifact.key: AppliedEntry(
                    artifact.fingerprint,
                    artifact.target.sql,
                    "now",
                    "run",
                    "applied",
                    artifact.fingerprint,
                    manifest.manifest_id,
                )
                for artifact in result.rendered
            }
        )
        self.markers = {
            artifact.target.sql: OwnershipMarker(manifest.manifest_id, artifact.fingerprint)
            for artifact in result.rendered
        }

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        self.opened.saw()
        _pause()
        if "SALES" in str(sql) or "STORES" in str(sql):
            raise SnowflakePortError("probe refused")
        return super().query(sql, params)

    def describe_marker(
        self, qualified_name: QualifiedName, object_type: str = "SEMANTIC VIEW"
    ) -> OwnershipMarker | None:
        self.opened.saw()
        _pause()
        return super().describe_marker(qualified_name, object_type)


def _smoke(result: CompileResult, manifest: Manifest, threads: int, *, foreign: str = "") -> tuple[SmokeResult, Opened]:
    opened = Opened()

    def session() -> SmokeSession:
        made = SmokeSession(opened, result, manifest)
        if foreign:
            made.markers[foreign] = OwnershipMarker("0" * 64, "1" * 64)
        return made

    first = session()
    with ConnectorPool(first, threads, session) as pool:
        outcome = SmokePublished(first, InMemoryStateStore(), Fanout(first, pool, threads)).run(
            result, manifest, target=dev_target(), state_table=STATE_TABLE
        )
    assert all(made.closed for made in opened.sessions if made is not first)
    return outcome, opened


def _smoke_report(outcome: SmokeResult) -> tuple[object, ...]:
    return (
        [probe.key for probe in outcome.attempted],
        [(item.code, item.subject, item.message) for item in outcome.diagnostics],
    )


def test_smoke_probes_report_the_same_on_one_session_and_on_four() -> None:
    result = compiled(*VIEWS)
    manifest = manifest_for(result, EMPTY_SOURCES)

    one, one_opened = _smoke(result, manifest, 1)
    four, four_opened = _smoke(result, manifest, 4)

    assert _smoke_report(one) == _smoke_report(four) == _smoke_report(_smoke(result, manifest, 4)[0])
    assert [item.code for item in one.diagnostics] == ["SST-APL100", "SST-APL100", "SST-APL006"]
    assert len(one_opened.sessions) == 1 and one_opened.threads == {"MainThread"}
    assert len(four_opened.sessions) > 1 and any(name.startswith("sst-worker") for name in four_opened.threads)


def test_marker_reads_report_the_same_on_one_session_and_on_four() -> None:
    result = compiled(*VIEWS)
    manifest = manifest_for(result, EMPTY_SOURCES)

    one, _ = _smoke(result, manifest, 1, foreign="DB.SCH.ITEMS")
    four, opened = _smoke(result, manifest, 4, foreign="DB.SCH.ITEMS")

    assert _smoke_report(one) == _smoke_report(four)
    assert [(item.code, item.context["artifact"]) for item in four.diagnostics] == [
        ("SST-APL012", "semantic_view:items")
    ]
    assert four.attempted == () and len(opened.sessions) > 1


# Each eval's agent, and what its run does: completes, ends FAILED, or cannot report its status.
OUTCOMES = {"alpha": "COMPLETED", "beta": "FAILED", "gamma": "unreadable", "delta": "COMPLETED"}


class EvalSession(TrackedSession):
    """Answers each run's status and results by the agent its run name carries."""

    def query_in_context(self, scope: SchemaScope, sql: Sql, params: object = None) -> QueryResult:
        del scope
        self.opened.saw()
        _pause()
        text = str(sql)
        if "EXECUTE_AI_EVALUATION('START'" in text:
            return QueryResult()
        assert isinstance(params, tuple)
        run_name = next(str(value) for value in params if str(value).startswith("EVAL_"))
        agent = run_name.split("_")[1].casefold()
        if "STATUS" in text:
            if OUTCOMES[agent] == "unreadable":
                raise SnowflakePortError(f"status of {run_name} unreadable")
            return QueryResult(STATUS_COLUMNS, ((run_name, agent.upper(), "CORTEX AGENT", OUTCOMES[agent], []),))
        return result_rows()


def _evals() -> tuple[CompiledEval, ...]:
    base = compiled_eval_of()
    return tuple(
        replace(
            base,
            resolved=replace(base.resolved, agent=replace(base.resolved.agent, name=agent)),
            agent_target=QualifiedName.parse(f"DB.S.{agent.upper()}"),
        )
        for agent in OUTCOMES
    )


def _run_evals(threads: int) -> tuple[EvalSuiteResult, Opened]:
    opened = Opened()
    first = EvalSession(opened)
    with ConnectorPool(first, threads, lambda: EvalSession(opened)) as pool:
        suite = RunEvalSuite(first, FixedClock(), sessions=pool).run(
            _evals(),
            options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z", poll_interval_ms=0),
            threads=threads,
        )
    assert all(made.closed for made in opened.sessions if made is not first)
    return suite, opened


def _eval_report(suite: EvalSuiteResult) -> tuple[object, ...]:
    return (
        eval_suite_json(suite),
        [(item.code, item.subject, item.message) for item in suite.diagnostics],
        [result.accepted for result in suite.evals],
    )


def test_evals_report_the_same_on_one_session_and_on_four() -> None:
    one, one_opened = _run_evals(1)
    four, four_opened = _run_evals(4)

    assert _eval_report(one) == _eval_report(four) == _eval_report(_run_evals(4)[0])
    assert [result.eval_key for result in four.evals] == ["eval:alpha", "eval:beta", "eval:gamma", "eval:delta"]
    assert [result.accepted for result in four.evals] == [True, False, False, True]
    assert len(one_opened.sessions) == 1 and one_opened.threads == {"MainThread"}
    assert len(four_opened.sessions) > 1 and any(name.startswith("sst-worker") for name in four_opened.threads)
