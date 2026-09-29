"""Immutable Cortex Agent evaluation authoring and result values."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Mapping

from .agent import AgentModel
from .diagnostic import D, Diagnostic, DiagnosticBag, Origin

SYSTEM_EVAL_METRICS = frozenset(
    ("tool_selection_accuracy", "tool_execution_accuracy", "answer_correctness", "logical_consistency")
)
SYSTEM_EVAL_METRIC_VERSION = "v3"
EVAL_PASS_STATUSES = frozenset(("COMPLETED",))
EVAL_RETRY_MIN = 0
EVAL_CONCURRENCY_MIN = 1
SUPPORTED_JUDGE_PLACEHOLDERS = frozenset(
    (
        "input",
        "output",
        "ground_truth",
        "tool_info",
        "start_timestamp",
        "duration",
        "span_id",
        "span_type",
        "span_name",
        "llm_model",
        "error",
        "status",
    )
)
EVAL_TERMINAL_STATUSES = frozenset(("COMPLETED", "PARTIALLY_COMPLETED", "INVOCATION_PARTIALLY_COMPLETED", "CANCELLED"))
_EVAL_NAME_TOKEN = re.compile(r"{{\s*(agent(?:\s*\|\s*upper)?|sha7|variant|ts)\s*}}")
_JUDGE_PLACEHOLDER = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}")
_RELATIVE_DATE = re.compile(
    r"\b(?:last|this|current|recent)\s+(?:day|week|month|quarter|year)\b|"
    r"\b(?:ytd|mtd|yesterday|today|tomorrow|q[1-4])\b|"
    r"\b(?:first|second|third|fourth)\s+quarter\b",
    re.IGNORECASE,
)
_NUMBER = re.compile(r"(?<![A-Za-z0-9_])[-+]?\d+(?:\.\d+)?%?(?![A-Za-z0-9_])")
_SCALE = re.compile(r"(-?\d+(?:\.\d+)?)\s+(?:to|and|through|-)\s+(-?\d+(?:\.\d+)?)", re.IGNORECASE)
_SCORE_OUTPUT = re.compile(
    r"(?:return|respond|output|produce).{0,80}(?:numeric\s+)?score|score\s+alone|numeric\s+score",
    re.IGNORECASE,
)
_REASONING_OUTPUT = re.compile(r"\b(?:reasoning|rationale|explain|explanation|justify|justification)\b", re.IGNORECASE)
_SYSTEM_INTENT_HINTS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "tool_selection_accuracy": ("tool selection", "select the correct tool", "correct tool"),
        "tool_execution_accuracy": ("tool execution", "tool input", "tool output"),
        "answer_correctness": ("answer correctness", "evaluate whether the answer is correct"),
        "logical_consistency": ("logical consistency", "internally consistent"),
    }
)


def render_eval_name_template(
    template: str,
    *,
    agent: str,
    sha7: str,
    variant: str | None = None,
    ts: str | None = None,
) -> str:
    """Resolve the closed eval-name token grammar without invoking Jinja."""

    values = {"agent": agent, "sha7": sha7, "variant": variant, "ts": ts}

    def replace(match: re.Match[str]) -> str:
        token = match.group(1)
        if "|" in token:
            return agent.upper()
        value = values[token]
        if value is None:
            raise ValueError(f"eval name token {token!r} has no value")
        return value

    rendered = _EVAL_NAME_TOKEN.sub(replace, template)
    if "{{" in rendered or "}}" in rendered:
        raise ValueError("eval name template contains an unsupported expression")
    return rendered


@dataclass(frozen=True, slots=True)
class ThresholdRange:
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True, slots=True)
class EvalInvocation:
    origin: Origin
    tool_name: str | None = None
    tool_input: str | None = None
    tool_output: str | None = None


@dataclass(frozen=True, slots=True)
class EvalGroundTruth:
    origin: Origin
    invocations: tuple[EvalInvocation, ...] | None = None
    output: str | None = None
    required_filters: tuple[str, ...] = ()
    immutable: bool | None = None
    immutable_reason: str | None = None
    extra: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def has_expectation(self) -> bool:
        return (
            self.invocations is not None or self.output is not None or bool(self.required_filters) or bool(self.extra)
        )


@dataclass(frozen=True, slots=True)
class EvalQuestion:
    origin: Origin
    question: str | None
    ground_truth: EvalGroundTruth | None


@dataclass(frozen=True, slots=True)
class EvalDataset:
    origin: Origin
    source_file: str
    agent: str | None
    description: str | None
    questions: tuple[EvalQuestion, ...]
    forbidden_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvalColumnMapping:
    query_text: str = "input_query"
    ground_truth: str = "ground_truth"


@dataclass(frozen=True, slots=True)
class EvalDatasetConfig:
    mint: str | None = None
    name_template: str | None = None
    source_table_template: str | None = None
    column_mapping: EvalColumnMapping = EvalColumnMapping()


@dataclass(frozen=True, slots=True)
class EvalSystemMetric:
    origin: Origin
    name: str | None
    version: str | None = None
    gate: bool | None = None
    threshold: ThresholdRange | None = None
    judge_model: str | None = None


@dataclass(frozen=True, slots=True)
class EvalRetention:
    audit: tuple[str, ...] = ()
    decision: tuple[str, ...] = ()
    decision_window_days: int | None = None


@dataclass(frozen=True, slots=True)
class EvalRunConfig:
    name_template: str | None = None
    label: str | None = None
    description: str | None = None
    variant: str | None = None
    retention: EvalRetention = EvalRetention()
    tier: str | None = None
    retry: int | None = None
    concurrency: int | None = None
    baseline_runs: int | None = None
    accept_statuses: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvalSweepConfig:
    enabled: bool = False
    models: tuple[str, ...] = ()
    target: str | None = None
    hold_constant: tuple[str, ...] = ()
    cost_reporting: bool = False


@dataclass(frozen=True, slots=True)
class EvalConfig:
    origin: Origin
    source_file: str
    agent: str | None
    agent_version: str | None
    dataset: EvalDatasetConfig | None
    system_metrics: tuple[EvalSystemMetric, ...]
    custom_metric_names: tuple[str, ...]
    run: EvalRunConfig | None
    sweep: EvalSweepConfig | None = None
    forbidden_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvalScoreRanges:
    min_score: tuple[int, int]
    median_score: tuple[int, int]
    max_score: tuple[int, int]


@dataclass(frozen=True, slots=True)
class CustomEvalMetric:
    origin: Origin
    source_file: str
    name: str
    description: str | None
    model: str | None
    score_ranges: EvalScoreRanges | None
    prompt: str | None
    gate_default: bool | None = None
    threshold_default: ThresholdRange | None = None
    enabled: bool = True
    meta: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class EvalDefaults:
    eval_tier: str | None = None
    metrics: tuple[str, ...] = ()
    metric_version: str | None = None
    judge_model: str | None = None
    agent_version: str | None = None
    retry: int | None = None
    concurrency: int | None = None
    baseline_runs: int | None = None
    min_dataset_rows: int | None = None
    retention: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedEval:
    agent: AgentModel
    dataset: EvalDataset
    config: EvalConfig
    custom_metrics: tuple[CustomEvalMetric, ...]

    @property
    def name(self) -> str:
        return self.agent.name

    @property
    def key(self) -> str:
        return f"eval:{self.agent.name.casefold()}"

    @property
    def source_files(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    self.dataset.source_file,
                    self.config.source_file,
                    *(metric.source_file for metric in self.custom_metrics),
                )
            )
        )

    @property
    def depends_on(self) -> tuple[str, ...]:
        return (self.agent.key,)


@dataclass(frozen=True, slots=True)
class EvalCatalog:
    evals: tuple[ResolvedEval, ...]
    metrics: tuple[CustomEvalMetric, ...]
    defaults: EvalDefaults = EvalDefaults()
    diagnostics: DiagnosticBag = DiagnosticBag()

    def metric(self, name: str) -> CustomEvalMetric | None:
        folded = name.casefold()
        return next((metric for metric in self.metrics if metric.name.casefold() == folded), None)


def validate_eval_catalog(
    catalog: EvalCatalog,
    *,
    agent_tool_names: Mapping[str, Iterable[str]] | None = None,
    allowed_models: Iterable[str] = (),
) -> DiagnosticBag:
    """Validate the static eval contract without consulting Snowflake or persisted state.

    ``agent_tool_names`` is the compiler projection of exact trace-facing names. The
    validator accepts the projection instead of reproducing name resolution here.
    """

    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    has_tool_projection = agent_tool_names is not None
    tool_names = {name.casefold(): frozenset(values) for name, values in (agent_tool_names or {}).items()}
    allowed = frozenset(model.casefold() for model in allowed_models)
    custom_name_counts: dict[str, int] = {}
    for metric in catalog.metrics:
        custom_name_counts[metric.name.casefold()] = custom_name_counts.get(metric.name.casefold(), 0) + 1
    for metric in catalog.metrics:
        diagnostics.extend(_validate_custom_metric(metric, custom_name_counts, allowed))
    dataset_names: dict[str, str] = {}
    run_names: dict[tuple[str, str], str] = {}
    for resolved in catalog.evals:
        diagnostics.extend(
            _validate_eval(
                resolved,
                catalog.defaults,
                tool_names,
                dataset_names,
                run_names,
                has_tool_projection=has_tool_projection,
            )
        )
    return DiagnosticBag(tuple(diagnostics))


def _validate_eval(
    resolved: ResolvedEval,
    defaults: EvalDefaults,
    agent_tool_names: Mapping[str, frozenset[str]],
    dataset_names: dict[str, str],
    run_names: dict[tuple[str, str], str],
    *,
    has_tool_projection: bool,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    subject = resolved.key
    dataset = resolved.dataset
    config = resolved.config
    agent_name = resolved.agent.name
    for declared, origin in ((dataset.agent, dataset.origin), (config.agent, config.origin)):
        if declared is not None and declared.casefold() != agent_name.casefold():
            diagnostics.append(D("SST-REF012", name=declared, origin=origin, subject=subject))
    if dataset.agent and config.agent and dataset.agent.casefold() != config.agent.casefold():
        diagnostics.append(D("SST-REF012", name=config.agent, origin=config.origin, subject=subject))
    for key in dataset.forbidden_keys:
        diagnostics.append(
            D(
                "SST-VAL703",
                artifact=dataset.source_file,
                key=key,
                origin=dataset.origin,
                subject=subject,
            )
        )
    for key in config.forbidden_keys:
        diagnostics.append(
            D(
                "SST-VAL703",
                artifact=config.source_file,
                key=key,
                origin=config.origin,
                subject=subject,
            )
        )

    exact_tool_names = agent_tool_names.get(agent_name.casefold(), frozenset())
    # An agent that did not compile has no projection. Its own diagnostics say
    # why; checking the dataset against an empty tool set would only repeat that
    # failure as one "absent tool" per expected invocation.
    has_tool_projection = has_tool_projection and agent_name.casefold() in agent_tool_names
    sample_questions = frozenset(resolved.agent.sample_questions)
    exercised: set[str] = set()
    for index, row in enumerate(dataset.questions):
        ground_truth = row.ground_truth
        if not row.question or ground_truth is None or not ground_truth.has_expectation:
            detail = "no question" if not row.question else "no expected field"
            diagnostics.append(
                D(
                    "SST-VAL705",
                    artifact=dataset.source_file,
                    index=index,
                    detail=detail,
                    origin=row.origin,
                    subject=subject,
                )
            )
        if row.question in sample_questions:
            diagnostics.append(
                D(
                    "SST-VAL707",
                    artifact=dataset.source_file,
                    index=index,
                    origin=row.origin,
                    subject=subject,
                )
            )
        texts = [row.question or ""]
        if ground_truth is not None:
            texts.extend(_ground_truth_text(ground_truth))
            diagnostics.extend(_validate_immutable_ground_truth(dataset, index, ground_truth, subject))
            if ground_truth.invocations is not None:
                for invocation in ground_truth.invocations:
                    if invocation.tool_name is None:
                        continue
                    exercised.add(invocation.tool_name)
                    if has_tool_projection and invocation.tool_name not in exact_tool_names:
                        diagnostics.append(
                            D(
                                "SST-VAL708",
                                artifact=dataset.source_file,
                                index=index,
                                name=invocation.tool_name,
                                value=agent_name,
                                origin=invocation.origin,
                                subject=subject,
                            )
                        )
                    if (
                        has_tool_projection
                        and any(name.casefold() == "web_search" for name in exact_tool_names)
                        and "web" in invocation.tool_name.casefold()
                        and invocation.tool_name != "web_search"
                    ):
                        diagnostics.append(
                            D(
                                "SST-VAL709",
                                artifact=dataset.source_file,
                                index=index,
                                name=invocation.tool_name,
                                origin=invocation.origin,
                                subject=subject,
                            )
                        )
        for text in texts:
            match = _RELATIVE_DATE.search(text)
            if match is not None:
                diagnostics.append(
                    D(
                        "SST-VAL706",
                        artifact=dataset.source_file,
                        index=index,
                        value=match.group(0),
                        origin=row.origin,
                        subject=subject,
                    )
                )
                break
    if defaults.min_dataset_rows is not None and len(dataset.questions) < defaults.min_dataset_rows:
        diagnostics.append(
            D(
                "SST-VAL710",
                artifact=dataset.source_file,
                count=len(dataset.questions),
                expected=defaults.min_dataset_rows,
                origin=dataset.origin,
                subject=subject,
            )
        )
    if has_tool_projection:
        uncovered = exact_tool_names - exercised
        diagnostics.append(
            D(
                "SST-VAL711",
                artifact=dataset.source_file,
                value=len(uncovered),
                origin=dataset.origin,
                subject=subject,
            )
        )
    diagnostics.append(D("SST-VAL712", artifact=dataset.source_file, origin=dataset.origin, subject=subject))
    diagnostics.extend(_validate_dataset_name(resolved, dataset_names))
    diagnostics.extend(_validate_eval_config(resolved, defaults, exact_tool_names, run_names))
    return tuple(diagnostics)


def _validate_dataset_name(resolved: ResolvedEval, dataset_names: dict[str, str]) -> tuple[Diagnostic, ...]:
    config = resolved.config.dataset
    if config is None or config.name_template is None:
        return ()
    diagnostics: list[Diagnostic] = []
    if not re.search(r"{{\s*agent(?:\s*\|\s*upper)?\s*}}", config.name_template):
        diagnostics.append(
            D(
                "SST-PRS002",
                artifact=resolved.config.source_file,
                field="dataset.name_template agent token",
                origin=resolved.config.origin,
                subject=resolved.key,
            )
        )
    try:
        rendered = render_eval_name_template(
            config.name_template,
            agent=resolved.agent.name,
            sha7="0000000",
            variant="ci",
            ts="20000101T000000Z",
        )
    except ValueError:
        return tuple(diagnostics)
    folded = rendered.casefold()
    other = dataset_names.get(folded)
    if other is not None:
        diagnostics.append(
            D(
                "SST-VAL701",
                artifact=rendered,
                value=other,
                origin=resolved.config.origin,
                subject=resolved.key,
            )
        )
    else:
        dataset_names[folded] = resolved.name
    if len(rendered) > 128:
        diagnostics.append(
            D(
                "SST-VAL702",
                artifact=rendered,
                size=len(rendered),
                origin=resolved.config.origin,
                subject=resolved.key,
            )
        )
    return tuple(diagnostics)


def _validate_eval_config(
    resolved: ResolvedEval,
    defaults: EvalDefaults,
    agent_tool_names: frozenset[str],
    run_names: dict[tuple[str, str], str],
) -> tuple[Diagnostic, ...]:
    config = resolved.config
    diagnostics: list[Diagnostic] = []
    subject = resolved.key
    effective_agent_version = config.agent_version or defaults.agent_version
    if effective_agent_version != "committed" and not (
        isinstance(effective_agent_version, str)
        and (
            effective_agent_version.startswith("alias:") or re.fullmatch(r"VERSION\$[1-9]\d*", effective_agent_version)
        )
    ):
        diagnostics.append(
            D(
                "SST-VAL719",
                artifact=resolved.name,
                found=effective_agent_version,
                origin=config.origin,
                subject=subject,
            )
        )
    configured_custom = {name.casefold() for name in config.custom_metric_names}
    resolved_custom = {metric.name.casefold() for metric in resolved.custom_metrics}
    for name in sorted(configured_custom - resolved_custom):
        diagnostics.append(D("SST-VAL721", artifact=resolved.name, name=name, origin=config.origin, subject=subject))
    if config.dataset is not None:
        for value in (config.dataset.name_template, config.dataset.source_table_template):
            if value is None:
                continue
            try:
                rendered = render_eval_name_template(
                    value,
                    agent=resolved.agent.name,
                    sha7="0000000",
                    variant="ci",
                    ts="20000101T000000Z",
                )
            except ValueError:
                continue
            if not rendered or len(rendered) > 128:
                diagnostics.append(
                    D(
                        "SST-PRS010",
                        name=rendered,
                        size=len(rendered),
                        expected=128,
                        origin=config.origin,
                        subject=subject,
                    )
                )
    if not config.system_metrics and not config.custom_metric_names:
        diagnostics.append(
            D(
                "SST-PRS101",
                artifact=config.source_file,
                field="metrics",
                origin=config.origin,
                subject=subject,
            )
        )
    has_threshold = False
    for metric in config.system_metrics:
        name = metric.name or ""
        if name not in SYSTEM_EVAL_METRICS:
            diagnostics.append(
                D("SST-VAL721", artifact=resolved.name, name=name, origin=metric.origin, subject=subject)
            )
            continue
        effective_version = metric.version or defaults.metric_version
        if effective_version is None:
            diagnostics.append(
                D(
                    "SST-VAL722",
                    artifact=resolved.name,
                    name=name,
                    origin=metric.origin,
                    subject=subject,
                )
            )
        elif effective_version != SYSTEM_EVAL_METRIC_VERSION:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=config.source_file,
                    field=f"metrics.system.{name}.version",
                    found=effective_version,
                    expected=SYSTEM_EVAL_METRIC_VERSION,
                    origin=metric.origin,
                    subject=subject,
                )
            )
            diagnostics.append(
                D(
                    "SST-VAL723",
                    artifact=resolved.name,
                    name=name,
                    found=effective_version,
                    origin=metric.origin,
                    subject=subject,
                )
            )
        if metric.judge_model is not None:
            diagnostics.append(
                D("SST-VAL724", artifact=resolved.name, name=name, origin=metric.origin, subject=subject)
            )
        if name == "tool_selection_accuracy":
            diagnostics.append(
                D(
                    "SST-VAL725",
                    artifact=resolved.name,
                    name=name,
                    detail="uses no LLM judge",
                    origin=metric.origin,
                    subject=subject,
                )
            )
        if name == "logical_consistency" and metric.gate:
            diagnostics.append(D("SST-VAL726", artifact=resolved.name, origin=metric.origin, subject=subject))
        if metric.gate:
            has_threshold = has_threshold or metric.threshold is not None
            if not _usable_threshold(metric.threshold):
                diagnostics.append(
                    D(
                        "SST-VAL733",
                        artifact=resolved.name,
                        name=name,
                        detail="no bound" if metric.threshold is None else "inverted bounds",
                        origin=metric.origin,
                        subject=subject,
                    )
                )
        elif metric.threshold is not None:
            has_threshold = True
            diagnostics.append(
                D(
                    "SST-VAL733",
                    artifact=resolved.name,
                    name=name,
                    detail="a threshold on an ungated metric",
                    origin=metric.origin,
                    subject=subject,
                )
            )
    run = config.run
    if run is None:
        return tuple(diagnostics)
    if run.name_template is not None:
        try:
            run_name = render_eval_name_template(
                run.name_template,
                agent=resolved.agent.name,
                sha7="0000000",
                variant=run.variant or "ci",
                ts="20000101T000000Z",
            )
        except ValueError:
            run_name = None
        if run_name is not None and "0000000" not in run_name:
            diagnostics.append(
                D(
                    "SST-VAL718",
                    artifact=resolved.name,
                    value=run_name,
                    origin=config.origin,
                    subject=subject,
                )
            )
        if run_name is not None:
            run_key = (resolved.agent.name.casefold(), run_name.casefold())
            if run_key in run_names:
                diagnostics.append(
                    D(
                        "SST-VAL717",
                        artifact=resolved.name,
                        value=run_name,
                        origin=config.origin,
                        subject=subject,
                    )
                )
            else:
                run_names[run_key] = resolved.name
    retry = run.retry if run.retry is not None else defaults.retry
    concurrency = run.concurrency if run.concurrency is not None else defaults.concurrency
    baseline_runs = run.baseline_runs if run.baseline_runs is not None else defaults.baseline_runs
    if retry is not None and retry < EVAL_RETRY_MIN:
        diagnostics.append(
            D(
                "SST-PRS016",
                artifact=config.source_file,
                field="run.retry",
                found=retry,
                expected=f">= {EVAL_RETRY_MIN}",
                origin=config.origin,
                subject=subject,
            )
        )
    if concurrency is not None and concurrency < EVAL_CONCURRENCY_MIN:
        diagnostics.append(
            D(
                "SST-PRS016",
                artifact=config.source_file,
                field="run.concurrency",
                found=concurrency,
                expected=f">= {EVAL_CONCURRENCY_MIN}",
                origin=config.origin,
                subject=subject,
            )
        )
    if defaults.concurrency is not None and run.concurrency is not None and run.concurrency > defaults.concurrency:
        diagnostics.append(
            D(
                "SST-VAL731",
                artifact=resolved.name,
                found=run.concurrency,
                expected=defaults.concurrency,
                origin=config.origin,
                subject=subject,
            )
        )
    if baseline_runs is not None and baseline_runs < 1:
        diagnostics.append(
            D(
                "SST-PRS016",
                artifact=config.source_file,
                field="run.baseline_runs",
                found=baseline_runs,
                expected=">= 1",
                origin=config.origin,
                subject=subject,
            )
        )
    if has_threshold and not (baseline_runs or 0) > 0:
        diagnostics.append(
            D("SST-VAL735", artifact=resolved.name, name="gated metric", origin=config.origin, subject=subject)
        )
    for status in run.accept_statuses:
        if status not in EVAL_PASS_STATUSES:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=config.source_file,
                    field="run.accept_statuses",
                    found=status,
                    expected="COMPLETED",
                    origin=config.origin,
                    subject=subject,
                )
            )
    skipped = sorted({tool.type for tool in resolved.agent.tools if tool.type in ("mcp", "agent")})
    for value in skipped:
        diagnostics.append(D("SST-VAL732", artifact=resolved.name, value=value, origin=config.origin, subject=subject))
    return tuple(diagnostics)


def _validate_custom_metric(
    metric: CustomEvalMetric,
    name_counts: Mapping[str, int],
    allowed_models: frozenset[str],
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    subject = f"eval_metric:{metric.name.casefold()}"
    if name_counts[metric.name.casefold()] > 1:
        diagnostics.append(D("SST-VAL001", type="eval_metric", name=metric.name, origin=metric.origin, subject=subject))
    if metric.name.casefold() in SYSTEM_EVAL_METRICS:
        diagnostics.append(
            D(
                "SST-VAL737",
                artifact=metric.name,
                name=metric.name,
                origin=metric.origin,
                subject=subject,
            )
        )
    if metric.threshold_default is not None and not metric.gate_default:
        diagnostics.append(
            D(
                "SST-VAL733",
                artifact=metric.name,
                name=metric.name,
                detail="a threshold on an ungated metric",
                origin=metric.origin,
                subject=subject,
            )
        )
    if metric.model is None or metric.model.casefold() == "auto":
        diagnostics.append(
            D(
                "SST-VAL738",
                artifact=metric.name,
                found=metric.model,
                origin=metric.origin,
                subject=subject,
            )
        )
    elif allowed_models and metric.model.casefold() not in allowed_models:
        diagnostics.append(
            D(
                "SST-VAL739",
                artifact=metric.name,
                found=metric.model,
                origin=metric.origin,
                subject=subject,
            )
        )
    prompt = metric.prompt or ""
    for system_name, hints in _SYSTEM_INTENT_HINTS.items():
        if system_name != metric.name.casefold() and any(hint in prompt.casefold() for hint in hints):
            diagnostics.append(
                D(
                    "SST-VAL740",
                    artifact=metric.name,
                    name=system_name,
                    origin=metric.origin,
                    subject=subject,
                )
            )
            break
    scale = _prompt_scale(prompt)
    has_output_contract = scale is not None and (
        _SCORE_OUTPUT.search(prompt) is not None or _prompt_anchors_declared_bands(prompt, metric.score_ranges)
    )
    if not has_output_contract:
        diagnostics.append(D("SST-VAL741", artifact=metric.name, origin=metric.origin, subject=subject))
    if _REASONING_OUTPUT.search(prompt) and not has_output_contract:
        diagnostics.append(D("SST-VAL742", artifact=metric.name, origin=metric.origin, subject=subject))
    has_tie_break = re.search(r"\b(?:tie|if tied|when tied)\b", prompt, re.IGNORECASE)
    has_insufficient = re.search(r"\b(?:insufficient|not enough|unclear|ambiguous)\b", prompt, re.IGNORECASE)
    if not has_tie_break and not has_insufficient:
        diagnostics.append(
            D(
                "SST-VAL743",
                artifact=metric.name,
                detail="tie-break or insufficient-information",
                origin=metric.origin,
                subject=subject,
            )
        )
    ranges = metric.score_ranges
    if ranges is not None:
        diagnostics.extend(_validate_score_ranges(metric, ranges, subject))
        scale_width = ranges.max_score[1] - ranges.min_score[0] + 1
        if "invariant" in f"{metric.description or ''} {prompt}".casefold() and scale_width > 3:
            diagnostics.append(
                D(
                    "SST-VAL748",
                    artifact=metric.name,
                    count=scale_width,
                    origin=metric.origin,
                    subject=subject,
                )
            )
        if scale is not None:
            declared = (ranges.min_score[0], ranges.max_score[1])
            if scale[0] < declared[0] or scale[1] > declared[1]:
                diagnostics.append(
                    D(
                        "SST-VAL747",
                        artifact=metric.name,
                        found=f"{scale[0]}..{scale[1]}",
                        expected=f"{declared[0]}..{declared[1]}",
                        origin=metric.origin,
                        subject=subject,
                    )
                )
        if metric.threshold_default is not None:
            threshold = metric.threshold_default
            scale_min = ranges.min_score[0]
            scale_max = ranges.max_score[1]
            usable = (
                threshold.min is not None
                and scale_min <= threshold.min <= scale_max
                and (threshold.max is None or scale_min <= threshold.max <= scale_max)
                and (threshold.max is None or threshold.min <= threshold.max)
            )
            if not usable:
                diagnostics.append(
                    D(
                        "SST-PRS115",
                        artifact=metric.name,
                        found=_threshold_text(threshold),
                        expected=f"{scale_min}..{scale_max}",
                        origin=metric.origin,
                        subject=subject,
                    )
                )
    for placeholder in _JUDGE_PLACEHOLDER.findall(prompt):
        if placeholder not in SUPPORTED_JUDGE_PLACEHOLDERS:
            diagnostics.append(
                D(
                    "SST-PRS116",
                    artifact=metric.name,
                    placeholder=placeholder,
                    origin=metric.origin,
                    subject=subject,
                )
            )
    return tuple(diagnostics)


def _validate_score_ranges(
    metric: CustomEvalMetric,
    ranges: EvalScoreRanges,
    subject: str,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    values = (*ranges.min_score, *ranges.median_score, *ranges.max_score)
    if any(low > high for low, high in (ranges.min_score, ranges.median_score, ranges.max_score)):
        diagnostics.append(
            D("SST-PRS114", artifact=metric.name, value="an inverted band", origin=metric.origin, subject=subject)
        )
    if ranges.min_score[1] + 1 != ranges.median_score[0] or ranges.median_score[1] + 1 != ranges.max_score[0]:
        diagnostics.append(
            D(
                "SST-PRS114",
                artifact=metric.name,
                value=f"{values}",
                origin=metric.origin,
                subject=subject,
            )
        )
    return tuple(diagnostics)


def _validate_immutable_ground_truth(
    dataset: EvalDataset,
    index: int,
    ground_truth: EvalGroundTruth,
    subject: str,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    if ground_truth.immutable_reason and not ground_truth.immutable:
        diagnostics.append(
            D(
                "SST-PRS015",
                artifact=dataset.source_file,
                field=f"questions[{index}].ground_truth.immutable_reason",
                other="immutable: true",
                origin=ground_truth.origin,
                subject=subject,
            )
        )
    if ground_truth.immutable and not ground_truth.immutable_reason:
        diagnostics.append(
            D(
                "SST-PRS015",
                artifact=dataset.source_file,
                field=f"questions[{index}].ground_truth.immutable",
                other="immutable_reason",
                origin=ground_truth.origin,
                subject=subject,
            )
        )
    if ground_truth.output and _NUMBER.search(ground_truth.output):
        if not ground_truth.immutable or not ground_truth.immutable_reason:
            diagnostics.append(
                D(
                    "SST-PRS015",
                    artifact=dataset.source_file,
                    field=f"questions[{index}].ground_truth.ground_truth_output",
                    other="immutable: true plus immutable_reason",
                    origin=ground_truth.origin,
                    subject=subject,
                )
            )
    return tuple(diagnostics)


def _ground_truth_text(value: EvalGroundTruth) -> tuple[str, ...]:
    parts = [value.output or "", *value.required_filters]
    if value.invocations is not None:
        for invocation in value.invocations:
            parts.extend((invocation.tool_input or "", invocation.tool_output or ""))
    parts.extend(str(extra) for extra in value.extra.values())
    return tuple(parts)


def _usable_threshold(value: ThresholdRange | None) -> bool:
    if value is None or (value.min is None and value.max is None):
        return False
    return value.min is None or value.max is None or value.min <= value.max


def _prompt_scale(prompt: str) -> tuple[float, float] | None:
    matches = tuple(_SCALE.finditer(prompt))
    if not matches:
        return None
    lower = min(float(match.group(1)) for match in matches)
    upper = max(float(match.group(2)) for match in matches)
    return (lower, upper) if lower <= upper else (upper, lower)


def _prompt_anchors_declared_bands(prompt: str, ranges: EvalScoreRanges | None) -> bool:
    if ranges is None:
        return False
    compact = prompt.casefold()
    return all(f"{low} to {high}" in compact for low, high in (ranges.min_score, ranges.median_score, ranges.max_score))


def _threshold_text(value: ThresholdRange) -> str:
    return f"min={value.min}, max={value.max}"


@dataclass(frozen=True, slots=True)
class EvalMetricResult:
    question_key: str
    metric_name: str
    score: float | None
    passed: bool


@dataclass(frozen=True, slots=True)
class EvalResultRow:
    question_key: str
    input_query: str
    metrics: tuple[EvalMetricResult, ...]


@dataclass(frozen=True, slots=True)
class EvalCostSummary:
    duration_ms: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    llm_call_count: int = 0


@dataclass(frozen=True, slots=True)
class EvalRunAttempt:
    run_name: str
    attempt: int
    terminal_status: str
    rows: tuple[EvalResultRow, ...] = ()
    cost: EvalCostSummary = EvalCostSummary()
    retrieval_error: str | None = None
    status_details: tuple[str, ...] = ()
    agent_version: str | None = None


@dataclass(frozen=True, slots=True)
class EvalBaselineMetric:
    question_key: str
    metric_name: str
    passed_attempts: tuple[bool, ...]
    score_range: tuple[float | None, float | None] | None = None


@dataclass(frozen=True, slots=True)
class EvalBaselineRecord:
    eval_key: str
    dataset_fingerprint: str
    config_fingerprint: str
    agent_version: str
    metric_versions: tuple[tuple[str, str], ...]
    metrics: tuple[EvalBaselineMetric, ...]
    run_names: tuple[str, ...]
    captured_at: str
    expires_at: str
    reason: str
    tier: str = "report"
    gate_policy: tuple[tuple[str, bool], ...] = ()


@dataclass(frozen=True, slots=True)
class EvalGateState:
    eval_key: str
    tier: str
    regression_count: int
    regressions: tuple[EvalRegression, ...]
    unresolved: bool
    evaluated_at: str
    run_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvalRegression:
    question_key: str
    metric_name: str


@dataclass(frozen=True, slots=True)
class EvalGateVerdict:
    tier: str
    regressions: tuple[EvalRegression, ...]
    passed: bool
    reason: str | None = None

    @property
    def regression_count(self) -> int:
        return len(self.regressions)
