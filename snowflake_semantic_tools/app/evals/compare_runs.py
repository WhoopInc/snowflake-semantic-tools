"""Run each model of a comparison: the pinned one on the release, each candidate on its alias.

Every run is an attempt of the eval suite (`RunEvalSuite`) on a copy of the compiled eval whose
config differs only in `agent_params.agent_version`, and whose run is named
`<SUBJECT>_CMP_<RUN ID>_<MODEL>_<n>`: the dataset, its version, the metrics and their judges are
the published ones, and the config is staged at its own content-addressed path as the suite
stages every config. The eval config has no model of its own, so a run's model is the one its
version's specification names. Every run of every eval starts at once. Nothing is gated, no
baseline is written, and no eval state is recorded.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from hashlib import sha256
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.progress import AttemptStarted, EvalProgress, EvalProgressEvent
from snowflake_semantic_tools.app.evals.run import EvalRunResult, EvalSuiteResult, RunEvalSuite
from snowflake_semantic_tools.domain.model.eval import EvalDefaults, EvalRunConfig
from snowflake_semantic_tools.domain.model.setting import CONFIG_SOURCE, Setting
from snowflake_semantic_tools.domain.render.compare import compare_run_name
from snowflake_semantic_tools.domain.render.eval import render_eval_config

# A retry's run name: the first attempt's, then `_R<attempt>`.
_RETRY = re.compile(r"_R\d+$")


@dataclass(frozen=True, slots=True)
class EvalRequest:
    """What one eval's comparison runs: its candidates, in the order asked, and the runs of each model."""

    candidates: tuple[str, ...]
    runs: int


@dataclass(frozen=True, slots=True)
class ComparedModel:
    """One model of one eval's comparison, and the committed version its runs evaluate.

    Attributes:
        role: `pinned` or `candidate`.
        version: The release, or the version a candidate was committed as.
        alias: The candidate's `EVAL_` alias, which its runs name the version by; None for
            the pinned model, whose runs name the release.
    """

    model: str
    role: str
    version: str
    alias: str | None = None

    @property
    def selector(self) -> str:
        """The `agent_version` setting its runs resolve: `alias:<alias>`, else the release's name."""
        return f"alias:{self.alias}" if self.alias else self.version


@dataclass(frozen=True, slots=True)
class PlannedEval:
    """One eval's comparison to run: the eval, its models, the pinned one first, and its runs of each."""

    compiled: CompiledEval
    models: tuple[ComparedModel, ...]
    runs: int


@dataclass(frozen=True, slots=True)
class ComparisonRuns:
    """What every run of a comparison returned.

    Attributes:
        results: Each model's runs of each eval, in run order, by `(eval key, model)`.
        run_names: Each model's run names, the first attempt's of each run, by `(eval key, model)`.
        suite: The suite result, which carries every run's diagnostics.
    """

    results: Mapping[tuple[str, str], tuple[EvalRunResult, ...]]
    run_names: Mapping[tuple[str, str], tuple[str, ...]]
    suite: EvalSuiteResult


def compared_eval(compiled: CompiledEval, model: ComparedModel, *, run_id: str, number: int) -> CompiledEval:
    """Return the eval as one run of `model` evaluates it: on its version, under its own run name.

    Its `agent_version` setting and rendered config name the model's version, and its run block
    names the run, so the suite resolves, stages and names it as it would any eval's.
    """
    resolved = compiled.resolved
    # MEASURED: a run whose agent_version is an alias evaluates the version carrying it, on the
    # model its specification names; the config has no model of its own.
    selector = model.selector
    name = compare_run_name(compiled.name, run_id, model.model, number)
    run = replace(resolved.config.run or EvalRunConfig(), name_template=name)
    settings = {**resolved.settings, "agent_version": Setting("agent_version", selector, CONFIG_SOURCE)}
    retargeted = replace(resolved, config=replace(resolved.config, run=run), settings=MappingProxyType(settings))
    config_yaml = render_eval_config(
        retargeted.config,
        retargeted.custom_metrics,
        agent_target=compiled.agent_target,
        dataset_target=compiled.dataset_target,
        agent_version=selector,
    )
    rendered = replace(
        compiled.rendered, config_yaml=config_yaml, config_fingerprint=sha256(config_yaml.encode("utf-8")).hexdigest()
    )
    return replace(compiled, resolved=retargeted, rendered=rendered)


def planned_runs(plans: Sequence[PlannedEval]) -> int:
    """Count the runs a comparison starts at once, which its session pool must hold."""
    return sum(len(plan.models) * plan.runs for plan in plans)


def run_comparison(
    suite: RunEvalSuite,
    plans: Sequence[PlannedEval],
    *,
    run_id: str,
    defaults: EvalDefaults,
    options: EvalRunOptions,
) -> ComparisonRuns:
    """Start every run of every eval at once, poll them all, and read each one's results.

    Each run retries as the eval's `retry` setting says, as any suite attempt does.

    Raises:
        EvalRunsInterrupted: as `RunEvalSuite.run` raises it.
    """
    variants = [
        (
            (plan.compiled.artifact_key, model.model),
            compare_run_name(plan.compiled.name, run_id, model.model, number),
            compared_eval(plan.compiled, model, run_id=run_id, number=number),
        )
        for plan in plans
        for model in plan.models
        for number in range(1, plan.runs + 1)
    ]
    total = max(1, len(variants))
    ran = suite.run(
        [variant for _, _, variant in variants],
        defaults=replace(defaults, concurrency=total),
        options=options,
        threads=total,
    )
    results: dict[tuple[str, str], list[EvalRunResult]] = {}
    names: dict[tuple[str, str], list[str]] = {}
    for (key, name, _), result in zip(variants, ran.evals, strict=True):
        results.setdefault(key, []).append(result)
        names.setdefault(key, []).append(name)
    return ComparisonRuns(
        MappingProxyType({key: tuple(value) for key, value in results.items()}),
        MappingProxyType({key: tuple(value) for key, value in names.items()}),
        ran,
    )


def labelled(progress: EvalProgress, plans: Sequence[PlannedEval], *, run_id: str) -> EvalProgress:
    """Label each run's progress `<eval> <model> run <n>`, in place of the eval key every copy shares."""
    labels = {
        compare_run_name(plan.compiled.name, run_id, model.model, number): (
            f"{plan.compiled.artifact_key} {model.model} run {number}"
        )
        for plan in plans
        for model in plan.models
        for number in range(1, plan.runs + 1)
    }

    def report(event: EvalProgressEvent) -> None:
        if isinstance(event, AttemptStarted):
            label = labels.get(_RETRY.sub("", event.run_name))
            event = replace(event, label=label) if label is not None else event
        progress(event)

    return report
