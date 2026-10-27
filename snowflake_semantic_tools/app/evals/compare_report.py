"""Turn a comparison's commits, runs and cost into its `sst.compare/1` report, eval by eval.

Each eval's models are its pinned model, then each candidate asked but the pinned one, in the
order asked: one whose version did not commit is reported failed, with why. A model's runs are
the suite's results for it, a run scoring once some attempt of it completed and was read. Its
lifecycle is read from SHOW CORTEX BASE MODELS, `unknown` when that read was refused.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareSubject, LifecycleResult
from snowflake_semantic_tools.app.evals.compare_runs import ComparisonRuns, EvalRequest
from snowflake_semantic_tools.app.evals.run import EvalRunResult
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.evaluate.compare import (
    CANDIDATE,
    PINNED,
    Dataset,
    EvalComparison,
    MetricSpec,
    ModelCost,
    ModelRuns,
    Subject,
    compare_eval,
    model_lifecycle,
)
from snowflake_semantic_tools.domain.evaluate.compare_json import CompareReport
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from snowflake_semantic_tools.domain.model.eval import EVAL_PASS_STATUSES, EvalRunAttempt
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel


@dataclass(frozen=True, slots=True)
class ModelEntry:
    """What one model's cost is read by: the agent requests its runs made, and the runs' names."""

    request_ids: tuple[str, ...]
    run_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ComparedEval:
    """One eval's comparison: the eval, its subject and release, what was asked, and what committed."""

    compiled: CompiledEval
    subject: CompareSubject
    release: str
    request: EvalRequest
    lifecycle: LifecycleResult

    @property
    def models(self) -> tuple[str, ...]:
        """The models compared: the pinned one, then each candidate asked but it, in the order asked."""
        pinned = self.subject.pinned_model
        return (pinned, *(model for model in self.request.candidates if model.casefold() != pinned.casefold()))

    def entries(self, ran: ComparisonRuns) -> tuple[tuple[str, ModelEntry], ...]:
        """Return each model's agent requests and run names, as its cost is read by."""
        key = self.compiled.artifact_key
        return tuple(
            (
                model,
                ModelEntry(
                    tuple(
                        sorted(
                            {
                                item
                                for attempt in _scored(ran.results.get((key, model), ()))
                                for row in attempt.rows
                                for item in row.request_ids
                            }
                        )
                    ),
                    ran.run_names.get((key, model), ()),
                ),
            )
            for model in self.models
        )


def comparison_report(
    compared: Sequence[ComparedEval],
    ran: ComparisonRuns,
    costs: Mapping[tuple[str, str], ModelCost],
    listed: Mapping[str, CortexBaseModel] | None,
    *,
    run_id: str,
    generated_at: str,
    target: str,
    git_sha: str,
    report_path: str,
) -> tuple[CompareReport, tuple[Diagnostic, ...]]:
    """Build the report of every eval compared, and what it notices, eval by eval.

    Args:
        listed: SHOW CORTEX BASE MODELS by casefolded model name; None when it was refused.

    Diagnostics:
        Those of `comparison_notices`, for each eval.
    """
    evaluations = tuple(_evaluation(item, ran, costs, listed) for item in compared)
    notices = tuple(notice for item in evaluations for notice in comparison_notices(item, path=report_path))
    return CompareReport(generated_at, generated_at, target, run_id, git_sha, evaluations), notices


def _evaluation(
    item: ComparedEval,
    ran: ComparisonRuns,
    costs: Mapping[tuple[str, str], ModelCost],
    listed: Mapping[str, CortexBaseModel] | None,
) -> EvalComparison:
    """Build one eval's comparison from its models' runs and cost."""
    compiled = item.compiled
    key = compiled.artifact_key
    committed = {candidate.model: candidate for candidate in item.lifecycle.candidates}
    failed = dict(item.lifecycle.failed)
    runs: list[ModelRuns] = []
    for model in item.models:
        results = ran.results.get((key, model), ())
        candidate = committed.get(model)
        pinned = model == item.subject.pinned_model
        version = item.release if pinned else candidate.version if candidate is not None else None
        runs.append(
            ModelRuns(
                model=model,
                role=PINNED if pinned else CANDIDATE,
                lifecycle=model_lifecycle(listed.get(model.casefold())) if listed is not None else "unknown",
                version=version,
                alias=candidate.alias if candidate is not None else None,
                run_names=ran.run_names.get((key, model), ()),
                scored=_scored(results),
                runs=item.request.runs,
                error=failed.get(model) or _failure(results),
            )
        )
    return compare_eval(
        eval_name=compiled.name,
        kind="cowork_profile" if compiled.resolved.subject.is_profile else "agent",
        subject=Subject(item.subject.name, item.subject.target.sql, item.release, item.subject.pinned_model),
        dataset=Dataset(
            compiled.dataset_target.name.value,
            compiled.dataset_version if compiled.mints else None,
            len(compiled.resolved.dataset.questions),
        ),
        runs_per_model=item.request.runs,
        metrics=metric_specs(compiled),
        runs=runs,
        costs={model: costs[(key, model)] for model in item.models if (key, model) in costs},
        notes=_notes(item),
    )


def metric_specs(compiled: CompiledEval) -> tuple[MetricSpec, ...]:
    """List the metrics an eval scores, in its config's order: system, then routing, then custom.

    A metric gates as the eval gate reads it: its `gate`, a custom metric's by its default.
    """
    config = compiled.resolved.config
    return (
        *(
            MetricSpec(metric.name, bool(metric.gate), metric.threshold)
            for metric in config.system_metrics
            if metric.name
        ),
        *(
            MetricSpec(metric.name, bool(metric.gate), metric.threshold)
            for metric in config.routing_metrics
            if metric.name
        ),
        *(
            MetricSpec(metric.name, bool(metric.gate_default), metric.threshold_default)
            for metric in compiled.resolved.custom_metrics
            if metric.enabled
        ),
    )


def _scored(results: Sequence[EvalRunResult]) -> tuple[EvalRunAttempt, ...]:
    """Return each run's last attempt that completed and was read, skipping a run with none."""
    return tuple(
        scored[-1]
        for result in results
        if (
            scored := [
                a for a in result.attempts if a.terminal_status in EVAL_PASS_STATUSES and a.retrieval_error is None
            ]
        )
    )


def _failure(results: Sequence[EvalRunResult]) -> str | None:
    """Say why the first run that never scored did not: its last attempt's status and reason."""
    for result in results:
        if result.accepted:
            continue
        attempt = result.attempts[-1] if result.attempts else None
        if attempt is None:
            return next((item.message for item in result.diagnostics), "the run did not start")
        reason = attempt.retrieval_error or "; ".join(attempt.status_details) or "no status details"
        return f"run {attempt.run_name} ended {attempt.terminal_status}: {reason}"
    return None


def _notes(item: ComparedEval) -> tuple[str, ...]:
    """Say what the comparison held fixed and committed, and what a CoWork profile's runs measure."""
    compiled = item.compiled
    judges = sorted({metric.model for metric in compiled.resolved.custom_metrics if metric.enabled and metric.model})
    dataset = (
        f"{compiled.dataset_target.name.value} {compiled.dataset_version}"
        if compiled.mints
        else compiled.dataset_target.name.value
    )
    held = (
        f"Held fixed: dataset {dataset}, metric versions, "
        f"{'judge ' + ', '.join(judges) if judges else 'the system metrics judges'}, "
        "everything in the specification but models.orchestration."
    )
    versions = [f"{candidate.version} ({candidate.alias})" for candidate in item.lifecycle.candidates]
    committed = ", ".join(
        part
        for part in (
            f"{item.lifecycle.hold} (hold)" if item.lifecycle.hold else "",
            *versions,
            f"{item.lifecycle.restore} (restore)" if item.lifecycle.restore else "",
        )
        if part
    )
    notes = [held]
    if committed:
        notes.append(f"Committed {committed}; the default and the release's alias stayed on {item.release}.")
    if any(model.casefold() == item.subject.pinned_model.casefold() for model in item.request.candidates):
        notes.append(f"{item.subject.pinned_model} is the pinned model, so it ran once, as pinned.")
    if compiled.resolved.subject.is_profile:
        notes.append(
            "Cost is the router's alone: the subagent runs it routed to are not attributed, and each "
            "subagent ran its live version."
        )
    return tuple(notes)
