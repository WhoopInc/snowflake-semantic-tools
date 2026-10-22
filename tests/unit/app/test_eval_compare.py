"""A whole model comparison of an agent's and a CoWork profile's eval, against the fake account.

The fake models each subject's versions and answers every run, so a test reads, in order, what
the comparison committed, ran, and reported, and checks the subject is left as apply left it.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import Any

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.compare import (
    CompareOutcome,
    RunEvalComparison,
    comparison_run_id,
    requested_models,
)
from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareSubject
from snowflake_semantic_tools.app.evals.compare_precheck import published_release
from snowflake_semantic_tools.app.evals.compare_refresh import refresh_cost
from snowflake_semantic_tools.app.evals.compare_runs import ComparedModel, EvalRequest, PlannedEval, planned_runs
from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, RunEvalSuite
from snowflake_semantic_tools.app.evals.suite import EvalGateRefused
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.compare import PENDING_NOTE
from snowflake_semantic_tools.domain.evaluate.compare_json import report_json
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, EvalDefaults
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage, JudgeUsage
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.compare_world import RELEASE, ComparingSnowflake, compare_subject, orchestration, publish
from tests.helpers.eval_builders import compile_eval, compile_profile_eval
from tests.helpers.eval_inputs import concierge_eval, settled
from tests.helpers.project_inputs import InMemoryProjectInputs

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
OPUS = "claude-opus-4-8"
GPT = "openai-gpt-5"


class Clock(FixedClock):
    """A clock whose times parse, and whose run id names the comparison `c7f2a19e`."""

    def now_iso(self) -> str:
        self.current += 1
        return f"2026-10-22T14:{self.current // 60:02d}:{self.current % 60:02d}Z"

    def new_run_id(self) -> str:
        return "c7f2a19e-0000-4000-8000-000000000000"


@pytest.fixture(autouse=True)
def published(monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat every eval as published; each subject's publication is what the precheck reads."""
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.compare.validate_eval_publication", lambda *args: DiagnosticBag()
    )


def agent_eval() -> CompiledEval:
    [compiled] = compile_eval().compiled
    assert isinstance(compiled, CompiledEval)
    return compiled


def profile_eval() -> CompiledEval:
    [compiled] = compile_profile_eval(settled(concierge_eval(), retry=0)).compiled
    assert isinstance(compiled, CompiledEval)
    return compiled


def world(
    *evals: CompiledEval, scores: dict[str, float] | None = None
) -> tuple[ComparingSnowflake, dict[str, CompareSubject]]:
    port = ComparingSnowflake(evals, scores=scores)
    subjects: dict[str, CompareSubject] = {}
    applied: dict[str, AppliedEntry] = {}
    for item in evals:
        subject = (
            compare_subject("concierge", kind="cowork_profile", target=item.agent_target.sql)
            if item.resolved.subject.is_profile
            else compare_subject(target=item.agent_target.sql)
        )
        applied[subject.key] = publish(port, subject)
        subjects[item.artifact_key] = subject
    port.remote_state = MappingProxyType(applied)
    port.listed_models = (
        CortexBaseModel(OPUS, "GA"),
        CortexBaseModel(GPT, "PUPR"),
        CortexBaseModel("claude-sonnet-4-6", "GA"),
    )
    return port, subjects


def compare(
    port: ComparingSnowflake,
    subjects: dict[str, CompareSubject],
    *evals: CompiledEval,
    candidates: tuple[str, ...] = (OPUS, GPT),
    runs: int = 2,
    lock: InMemoryStateStore | None = None,
) -> CompareOutcome | EvalGateRefused:
    inputs = InMemoryProjectInputs(revision="abcdef0", evals=EvalCatalog((), (), EvalDefaults()))
    use_case = RunEvalComparison(port, inputs, lock or InMemoryStateStore(), Clock())
    requests = {item.artifact_key: EvalRequest(candidates, runs) for item in evals}
    manifest = build_manifest(compile_eval())
    return use_case.run(
        evals,
        subjects,
        requests,
        manifest,
        target=target(),
        state_table=STATE_TABLE,
        report_path="sst_target/compare/c.json",
    )


def sent(port: ComparingSnowflake) -> list[str]:
    return [text for script in port.scripts for text in script if text.startswith("ALTER AGENT")]


def test_an_agents_comparison_commits_runs_every_model_and_reports_the_winner() -> None:
    item = agent_eval()
    port, subjects = world(item, scores={"claude_sonnet_4_6": 0.4, "claude_opus_4_8": 0.95, "openai_gpt_5": 1.0})
    outcome = compare(port, subjects, item)
    assert isinstance(outcome, CompareOutcome) and outcome.passed, outcome.diagnostics
    agent = "DB.S.SALES_AGENT"
    statements = sent(port)
    assert [text.split("\n")[0].split(" = ")[0] for text in statements] == [
        f"ALTER AGENT {agent} COMMIT COMMENT",
        f"ALTER AGENT {agent}",
        f"ALTER AGENT {agent} MODIFY VERSION VERSION$6 SET ALIAS",
        f"ALTER AGENT {agent}",
        f"ALTER AGENT {agent} MODIFY VERSION VERSION$7 SET ALIAS",
        f"ALTER AGENT {agent}",
        f"ALTER AGENT {agent} ADD LIVE VERSION FROM LAST",
    ]
    assert not any("DEFAULT_VERSION" in text or "RELEASED" in text for text in statements)
    # Every run of every model started, the pinned model's on the release by name.
    assert sorted(port.started) == sorted(
        f"SALES_AGENT_CMP_C7F2A19E_{slug}_{number}"
        for slug in ("CLAUDE_SONNET_4_6", "CLAUDE_OPUS_4_8", "OPENAI_GPT_5")
        for number in (1, 2)
    )
    configs = {
        path: content.decode() for path, content in port.uploads if path.endswith(".yaml") and "/eval/" not in path
    }
    versions = sorted(
        line.strip() for text in configs.values() for line in text.splitlines() if "agent_version" in line
    )
    assert versions == [
        'agent_version: "EVAL_CLAUDE_OPUS_4_8"',
        'agent_version: "EVAL_OPENAI_GPT_5"',
        'agent_version: "VERSION$4"',
    ]
    assert outcome.report is not None
    evaluation: Any = json.loads(json.dumps(report_json(outcome.report)))["evals"][0]
    assert [
        (model["model"], model["role"], model["version"], model["alias"], model["lifecycle"])
        for model in evaluation["models"]
    ] == [
        ("claude-sonnet-4-6", "pinned", RELEASE, None, "ga"),
        (GPT, "candidate", "VERSION$7", "EVAL_OPENAI_GPT_5", "preview"),
        (OPUS, "candidate", "VERSION$6", "EVAL_CLAUDE_OPUS_4_8", "ga"),
    ]
    # The preview model scores best but is excluded; the GA candidate wins outside the spread.
    assert evaluation["verdict"]["kind"] == "winner" and evaluation["verdict"]["model"] == OPUS
    assert evaluation["verdict"]["excluded"] == [{"model": GPT, "reason": "preview"}]
    # Nothing has landed yet: the agent's usage is pending, and no eval span names a judge's.
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL773", "SST-VAL774", "SST-VAL775"]
    # The subject is left as apply left it: a plan of it is NOOP, served from the release.
    subject = subjects[item.artifact_key]
    entry = port.remote_state[subject.key] if port.remote_state else None
    assert published_release(port, subject, entry, fetched_at="now") == RELEASE
    live = next(row for row in port.agent_version_rows[agent] if row.name is None)
    assert orchestration(live.specification) == "claude-sonnet-4-6"


def test_a_profiles_comparison_scores_routing_and_notes_its_cost_is_the_routers() -> None:
    item = profile_eval()
    port, subjects = world(item)
    outcome = compare(port, subjects, item, candidates=(OPUS,), runs=1)
    assert isinstance(outcome, CompareOutcome) and outcome.passed, outcome.diagnostics
    assert "@DB.S.AGENT_SPECS/cowork-profiles/concierge/eval/c7f2a19e/claude_opus_4_8/" in sent(port)[1]
    assert outcome.report is not None
    [evaluation] = outcome.report.evals
    assert evaluation.kind == "cowork_profile"
    assert [metric.name for metric in evaluation.metrics] == ["answer_correctness", "routing_accuracy"]
    assert dict(evaluation.questions[0].results)[OPUS].route == ("DB.AGENTS.SALES",)
    assert "SST-VAL776" in [notice.code for notice in outcome.diagnostics]
    assert any(note.startswith("Cost is the router's alone") for note in evaluation.notes)


def test_cost_that_has_not_landed_is_pending_and_one_that_has_is_known() -> None:
    item = agent_eval()
    port, subjects = world(item)
    run = "SALES_AGENT_CMP_C7F2A19E_CLAUDE_SONNET_4_6_1"
    port.agent_usage_rows[f"{run}-q1"] = AgentUsage(f"{run}-q1", 0.5, None, "2026-10-22T14:00:00+00:00")
    port.judge_usage_rows[run] = JudgeUsage(run, 2, 2, 800)
    outcome = compare(port, subjects, item, candidates=(OPUS,), runs=1)
    assert isinstance(outcome, CompareOutcome) and outcome.report is not None
    [evaluation] = outcome.report.evals
    pinned, candidate = evaluation.models
    assert (pinned.cost.agent_credits.value, pinned.cost.sql_credits.state, pinned.cost.judge_tokens.value) == (
        0.5,
        "pending",
        800.0,
    )
    assert candidate.cost.agent_credits.state == "pending"
    assert [notice.code for notice in outcome.diagnostics] == ["SST-VAL773", "SST-VAL774"]
    assert (
        outcome.diagnostics[0].suggestion
        == "run `sst test --refresh-cost sst_target/compare/c.json` in about an hour to fill it in"
    )


def test_usage_the_role_may_not_read_is_unavailable_and_the_report_still_stands() -> None:
    item = agent_eval()
    port, subjects = world(item)
    port.fail("agent_usage", SnowflakePortError("Insufficient privileges to operate on ACCOUNT_USAGE\nmore"))
    port.fail("base_models", SnowflakePortError("SHOW refused"))
    outcome = compare(port, subjects, item, candidates=(OPUS,), runs=1)
    assert isinstance(outcome, CompareOutcome) and outcome.passed and outcome.report is not None
    [notice] = [notice for notice in outcome.diagnostics if notice.code == "SST-VAL774"]
    assert notice.message.endswith("is unavailable: Insufficient privileges to operate on ACCOUNT_USAGE")
    assert {model.lifecycle for model in outcome.report.evals[0].models} == {"unknown"}


def test_a_subject_not_published_as_compiled_commits_nothing() -> None:
    item = agent_eval()
    port, subjects = world(item)
    port.agent_default_settings[item.agent_target.sql] = "LAST"
    outcome = compare(port, subjects, item)
    assert isinstance(outcome, CompareOutcome) and not outcome.passed and outcome.report is None
    assert [diagnostic.code for diagnostic in outcome.diagnostics] == ["SST-APL108"]
    assert sent(port) == [] and port.started == []


def test_a_candidate_that_did_not_commit_is_reported_failed_and_the_rest_still_run() -> None:
    item = agent_eval()
    port, subjects = world(item)
    port.refused = ("/openai_gpt_5/",)
    outcome = compare(port, subjects, item, runs=1)
    assert isinstance(outcome, CompareOutcome) and not outcome.passed and outcome.report is not None
    models = {model.model: model for model in outcome.report.evals[0].models}
    assert models[GPT].status == "failed" and models[GPT].version is None
    assert (models[GPT].error or "").startswith("ADD VERSION failed")
    assert not any("OPENAI" in name for name in port.started)
    assert outcome.diagnostics[0].code == "SST-APL109"
    held, held_subjects = world(item)
    held.refused = ("COMMIT COMMENT",)
    refused = compare(held, held_subjects, item, runs=1)
    assert isinstance(refused, CompareOutcome) and refused.report is not None
    assert not any(note.startswith("Committed") for note in refused.report.evals[0].notes)


def test_a_run_that_failed_reports_why_and_the_comparison_still_passes() -> None:
    item = agent_eval()
    port, subjects = world(item)
    port.failing = {"claude_opus_4_8"}
    outcome = compare(port, subjects, item, candidates=(OPUS,), runs=1)
    assert isinstance(outcome, CompareOutcome) and outcome.report is not None
    opus = outcome.report.evals[0].models[1]
    assert opus.status == "failed" and (opus.error or "").startswith("run SALES_AGENT_CMP_C7F2A19E_CLAUDE_OPUS_4_8_1")
    assert outcome.report.evals[0].verdict.kind == "incomplete"


def test_a_comparison_waits_for_no_other_run_holding_the_target_lock() -> None:
    item = agent_eval()
    port, subjects = world(item)
    held = InMemoryStateStore()
    held.acquire_lock("apply-run", break_stale=False)
    refused = compare(port, subjects, item, lock=held)
    assert isinstance(refused, EvalGateRefused)
    assert refused.reason == "cannot compare models while apply-run holds the target lock" and port.started == []


def test_requested_models_take_the_command_line_then_the_setting_then_two_runs() -> None:
    item = agent_eval()
    assert requested_models((item,), given=(OPUS,), runs=None, allowed=frozenset()) == {
        item.artifact_key: EvalRequest((OPUS,), 2)
    }
    configured = settled(item.resolved, candidate_models=[GPT], compare_runs=3)
    tuned = replace(item, resolved=configured)
    assert requested_models((tuned,), given=None, runs=None, allowed=frozenset()) == {
        item.artifact_key: EvalRequest((GPT,), 3)
    }
    none = requested_models((item,), given=None, runs=None, allowed=frozenset())
    assert isinstance(none, Diagnostic) and none.code == "SST-VAL772"
    auto = requested_models((item,), given=("auto",), runs=1, allowed=frozenset())
    assert isinstance(auto, Diagnostic) and auto.code == "SST-VAL770"


def test_a_clocks_run_id_is_shortened_to_eight_letters_or_digits() -> None:
    assert comparison_run_id("C7F2A19E-0000-4000") == "c7f2a19e"
    assert comparison_run_id("---") == "run"


def test_a_run_that_never_started_reports_its_setup_and_the_pinned_model_once_as_pinned() -> None:
    item = agent_eval()
    config = item.resolved.config
    assert config.dataset is not None
    unminted = replace(
        item, resolved=replace(item.resolved, config=replace(config, dataset=replace(config.dataset, mint="never")))
    )
    port, subjects = world(unminted)
    port.fail("resolve_agent_version", SnowflakePortError("DESCRIBE AGENT refused"))
    outcome = compare(port, subjects, unminted, candidates=("Claude-Sonnet-4-6", OPUS), runs=1)
    assert isinstance(outcome, CompareOutcome) and outcome.report is not None
    [evaluation] = outcome.report.evals
    assert [model.model for model in evaluation.models] == ["claude-sonnet-4-6", OPUS]
    assert "DESCRIBE AGENT refused" in (evaluation.models[0].error or "")
    assert evaluation.dataset.version is None and evaluation.verdict.kind == "incomplete"
    assert "claude-sonnet-4-6 is the pinned model, so it ran once, as pinned." in evaluation.notes
    assert evaluation.notes[0].startswith(f"Held fixed: dataset {item.dataset_target.name.value}, ")


def test_the_pool_a_comparison_runs_on_holds_one_session_per_run() -> None:
    item = agent_eval()
    pinned = ComparedModel("claude-sonnet-4-6", "pinned", RELEASE)
    assert planned_runs((PlannedEval(item, (pinned, ComparedModel(OPUS, "candidate", "VERSION$6", "EVAL_X")), 3),)) == 6


def test_a_pending_cost_fills_in_when_the_report_is_refreshed() -> None:
    item = agent_eval()
    port, subjects = world(item, scores={"claude_opus_4_8": 0.5})
    outcome = compare(port, subjects, item, candidates=(OPUS,), runs=1)
    assert isinstance(outcome, CompareOutcome) and outcome.report is not None
    document = json.loads(json.dumps(report_json(outcome.report)))
    assert {model["cost"]["agent_credits"]["state"] for model in document["evals"][0]["models"]} == {"pending"}
    for model in outcome.report.evals[0].models:
        for request in model.request_ids:
            port.agent_usage_rows[request] = AgentUsage(request, 0.25, 0.0, "2026-10-22T14:00:00+00:00")
        for run in model.run_names:
            port.judge_usage_rows[run] = JudgeUsage(run, 1, 1, 100)
    refreshed = refresh_cost(port, document, path="c.json", now="2026-10-22T16:00:00Z")
    assert not isinstance(refreshed, Diagnostic)
    report, notices = refreshed
    assert report.cost_read_at == "2026-10-22T16:00:00Z" and notices == ()
    pinned = report.evals[0].models[0]
    assert (pinned.cost.agent_credits.state, pinned.cost.credits_per_question.value) == ("known", 0.25)
    assert PENDING_NOTE not in report.evals[0].notes
    # Nothing was run or committed again.
    assert len(port.started) == 2


def test_every_run_of_every_model_starts_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: dict[str, object] = {}
    original = RunEvalSuite.run

    def recorded(self: RunEvalSuite, compiled: Sequence[CompiledEval], **options: Any) -> EvalSuiteResult:
        asked.update(options, count=len(compiled))
        return original(self, compiled, **options)

    monkeypatch.setattr(RunEvalSuite, "run", recorded)
    item = agent_eval()
    port, subjects = world(item)
    outcome = compare(port, subjects, item, candidates=(OPUS, GPT), runs=3)
    assert isinstance(outcome, CompareOutcome) and outcome.passed
    # One suite run holds every run, paced to start them all: three models, three runs each.
    assert asked["count"] == asked["threads"] == 9
    defaults = asked["defaults"]
    assert isinstance(defaults, EvalDefaults) and defaults.concurrency == 9
    assert asked.get("baseline_capture", False) is False and asked.get("fail_fast", False) is False
