"""Eval authoring values: datasets, run configs, custom judges, and the catalog that joins them.

The YAML adapter parses each agent's eval files into these frozen values, and
`ResolvedEval` joins one agent to its dataset, its config and the custom metrics that
config names. `EvalCatalog` holds every eval with the project's `evals:` defaults. The
constants are the eval contract: the system metrics and the one version accepted for
them, the run statuses, and the placeholders a judge prompt may use. Nothing here reads
a file or calls Snowflake.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag, Origin

SYSTEM_EVAL_METRICS = frozenset(
    ("tool_selection_accuracy", "tool_execution_accuracy", "answer_correctness", "logical_consistency")
)
SYSTEM_EVAL_METRIC_VERSION = "v3"
# The run status Snowflake reports for an evaluation that finished every question.
EVAL_COMPLETED = "COMPLETED"
EVAL_PASS_STATUSES = frozenset((EVAL_COMPLETED,))
# The stage in each agent's schema that holds its eval run configs.
DEFAULT_EVAL_CONFIG_STAGE = "EVAL_CONFIGS"
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
EVAL_TERMINAL_STATUSES = frozenset(
    (EVAL_COMPLETED, "PARTIALLY_COMPLETED", "INVOCATION_PARTIALLY_COMPLETED", "CANCELLED")
)


@dataclass(frozen=True, slots=True)
class ThresholdRange:
    """A metric's pass band; a None bound leaves that side open."""

    min: float | None = None
    max: float | None = None


@dataclass(frozen=True, slots=True)
class EvalInvocation:
    """One expected tool call in a row's `ground_truth_invocations`.

    Attributes:
        tool_name: The agent's resolved, trace-facing tool name, matched exactly (case
            included); None when omitted.
        tool_input: Prose describing the intended call, not SQL; None when omitted.
        tool_output: Prose describing what the call should return; None when omitted.
    """

    origin: Origin
    tool_name: str | None = None
    tool_input: str | None = None
    tool_output: str | None = None


@dataclass(frozen=True, slots=True)
class EvalGroundTruth:
    """What one dataset row expects of the agent.

    Attributes:
        invocations: None when `ground_truth_invocations` is absent; `()` when it is an
            empty list, which is still an expectation: that no tool is called.
        output: `ground_truth_output`, the rubric a reply is judged against.
        immutable: Declared together with `immutable_reason`, and both are required when
            `output` contains a number in digits.
        extra: Keys the parser does not model, passed through into the rendered dataset.
    """

    origin: Origin
    invocations: tuple[EvalInvocation, ...] | None = None
    output: str | None = None
    required_filters: tuple[str, ...] = ()
    immutable: bool | None = None
    immutable_reason: str | None = None
    extra: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def has_expectation(self) -> bool:
        """Report whether the row states anything a run can be scored against."""
        return (
            self.invocations is not None or self.output is not None or bool(self.required_filters) or bool(self.extra)
        )


@dataclass(frozen=True, slots=True)
class EvalQuestion:
    """One dataset row; `question` is None when missing, `ground_truth` when absent or malformed."""

    origin: Origin
    question: str | None
    ground_truth: EvalGroundTruth | None


@dataclass(frozen=True, slots=True)
class EvalDataset:
    """An agent's eval questions, as authored in its dataset file.

    Attributes:
        agent: The agent the file declares; it must name the agent that points to it.
        description: For reviewers only; never rendered into anything sent to Snowflake.
        forbidden_keys: Which of `database`, `schema` and `enabled` the file sets; eval
            objects always live in the agent's schema, so each is rejected.
    """

    origin: Origin
    source_file: str
    agent: str | None
    description: str | None
    questions: tuple[EvalQuestion, ...]
    forbidden_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvalColumnMapping:
    """The source table's column names for the question and its ground truth."""

    query_text: str = "input_query"
    ground_truth: str = "ground_truth"


@dataclass(frozen=True, slots=True)
class EvalDatasetConfig:
    """How an eval's dataset objects are named, from the config's `dataset:` block.

    Attributes:
        mint: Parsed and carried, but it does not change how datasets are created.
        name_template: The DATASET name template; None when unset, which validation rejects.
        source_table_template: The source table name template; None when unset, likewise.
    """

    mint: str | None = None
    name_template: str | None = None
    source_table_template: str | None = None
    column_mapping: EvalColumnMapping = EvalColumnMapping()


@dataclass(frozen=True, slots=True)
class EvalSystemMetric:
    """One Snowflake system metric an eval config enables.

    Attributes:
        name: None when omitted; an unknown name is reported, not dropped.
        version: None falls back to `evals.+metric_version`.
        gate: Whether the metric gates the eval; None reads as ungated.
        judge_model: Must stay None: a system metric's judge comes with its version.
    """

    origin: Origin
    name: str | None
    version: str | None = None
    gate: bool | None = None
    threshold: ThresholdRange | None = None
    judge_model: str | None = None


@dataclass(frozen=True, slots=True)
class EvalRetention:
    """Which run variants are kept as the audit trail, and which only for a decision window.

    SST never deletes eval runs or eval objects on the strength of it.
    """

    audit: tuple[str, ...] = ()
    decision: tuple[str, ...] = ()
    decision_window_days: int | None = None


@dataclass(frozen=True, slots=True)
class EvalRunConfig:
    """How an eval runs, from the config's `run:` block.

    Attributes:
        name_template: The run name template; None uses the runner's default template.
        variant: Why the run happened; fills the `variant` token, and None reads as "ci".
        tier: `blocking` or `report`; None falls back to `evals.+eval_tier`, then `report`.
        retry: None falls back to `evals.+retry`; likewise `concurrency` and `baseline_runs`.
        accept_statuses: Terminal statuses the author accepts; only COMPLETED passes.
    """

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
    """A model sweep block; SST parses it but does not run sweeps."""

    enabled: bool = False
    models: tuple[str, ...] = ()
    target: str | None = None
    hold_constant: tuple[str, ...] = ()
    cost_reporting: bool = False


@dataclass(frozen=True, slots=True)
class EvalConfig:
    """An agent's eval run configuration, as authored in its config file.

    Attributes:
        agent_version: `committed`, `alias:<name>` or `VERSION$<n>`; None falls back to
            `evals.+agent_version`.
        dataset: None when the `dataset:` block is absent or not a mapping; likewise `run`
            and `sweep` for theirs.
        custom_metric_names: The names the `eval_metric()` references give, as written.
        forbidden_keys: Which of `database`, `schema` and `enabled` the file sets.
    """

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
    """A judge's three score bands, each an inclusive `(low, high)` pair of integers."""

    min_score: tuple[int, int]
    median_score: tuple[int, int]
    max_score: tuple[int, int]


@dataclass(frozen=True, slots=True)
class CustomEvalMetric:
    """A custom LLM-judge metric; it creates no Snowflake object and is inlined into eval configs.

    Attributes:
        model: The judge model; None and `auto` are rejected.
        score_ranges: None when absent or malformed; the parser reports why.
        gate_default: Whether every eval that uses the metric gates on it.
        enabled: False leaves the metric out of rendered eval configs.
        meta: Free-form annotations, never rendered.
    """

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
    """The project's `evals:` defaults from `sst_config.yml`; None, or `()`, means the key is unset."""

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
    """One agent's eval: the agent, its dataset and config, and the custom metrics that resolved.

    A metric the config names but the catalog lacks is absent from `custom_metrics`; the
    loader reports it, and validation reports it again against the config.
    """

    agent: AgentModel
    dataset: EvalDataset
    config: EvalConfig
    custom_metrics: tuple[CustomEvalMetric, ...]

    @property
    def name(self) -> str:
        """Name the eval after its agent."""
        return self.agent.name

    @property
    def key(self) -> str:
        """Return the eval's artifact key, from the casefolded agent name."""
        return artifact_key("eval", self.agent.name.casefold())

    @property
    def source_files(self) -> tuple[str, ...]:
        """List the dataset, config and custom metric files, first occurrence of each, in that order."""
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
        """Return the artifact keys the eval depends on: its agent's alone."""
        return (self.agent.key,)


@dataclass(frozen=True, slots=True)
class EvalCatalog:
    """Every eval and custom metric in a project, with the `evals:` defaults.

    Attributes:
        diagnostics: Reported about the catalog so far; validation returns these first, then its own.
    """

    evals: tuple[ResolvedEval, ...]
    metrics: tuple[CustomEvalMetric, ...]
    defaults: EvalDefaults = EvalDefaults()
    diagnostics: DiagnosticBag = DiagnosticBag()

    def metric(self, name: str) -> CustomEvalMetric | None:
        """Return the first custom metric whose name matches casefolded, or None."""
        folded = name.casefold()
        return next((metric for metric in self.metrics if metric.name.casefold() == folded), None)
