"""The key tables of eval files: a custom metric's judge, a dataset's questions, and a run's config."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_MINT_POLICIES,
    ROUTING_EVAL_METRICS,
    SYSTEM_EVAL_METRICS,
)

_STRING, _LIST, _MAPPING, _BOOLEAN = ValueKind.STRING, ValueKind.LIST, ValueKind.MAPPING, ValueKind.BOOLEAN


def _band(name: str, which: str) -> Field:
    return Field(
        name, _LIST, f"The inclusive {which} score band, as two integers.", required=True, items=ValueKind.INTEGER
    )


_SCORE_RANGES = Node(
    (_band("min_score", "low"), _band("median_score", "middle"), _band("max_score", "high")),
    open=True,
)
_THRESHOLD = Node(
    (
        Field("min", ValueKind.NUMBER, "The lowest passing score."),
        Field("max", ValueKind.NUMBER, "The highest passing score."),
    ),
    open=True,
)
EVAL_METRIC = FileKind(
    "eval_metric",
    "A custom eval metric: an LLM judge that scores each answer against a rubric.",
    "a YAML file in the eval metrics directory",
    Node(
        (
            Field(
                "name", _STRING, "The metric's name, which eval configs reference with `eval_metric()`.", required=True
            ),
            Field("description", _STRING, "What the judge measures."),
            Field("model", _STRING, "The judge model that scores the metric."),
            Field(
                "score_ranges",
                _MAPPING,
                "The judge's three score bands: low, middle and high.",
                required=True,
                node=_SCORE_RANGES,
            ),
            Field("prompt", _STRING, "The judge's rubric: its scale, how to report a score, and how to break ties."),
            Field("gate_default", _BOOLEAN, "Gates the metric in every eval that uses it."),
            Field("threshold_default", _MAPPING, "The default pass band of the metric's score.", node=_THRESHOLD),
            Field("enabled", _BOOLEAN, "`false` leaves the metric out of every eval config."),
            Field("meta", _MAPPING, "Free-form metadata, never rendered."),
        )
    ),
)

_TEMPLATE = ValueKind.TEMPLATE
_EVALS = "an agent's or CoWork profile's `evals/` folder"
_AGENT = "an eval belongs to the agent whose evals/ folder holds it, so its files name none"


def _located(key: str) -> Field:
    return Field(
        key,
        ValueKind.ANY,
        f"Refused: an eval's {key} comes from its `config:` and the `evals:` block.",
        status=FieldStatus.REFUSED,
        code="SST-VAL703",
    )


def _moved(name: str, reason: str) -> Field:
    return Field(name, ValueKind.ANY, f"No longer read: {reason}.", status=FieldStatus.REMOVED, replacement=reason)


# Fields both an eval dataset and an eval config refuse or no longer read.
_REFUSED = (
    _located("database"),
    _located("schema"),
    _located("enabled"),
    Field("agent", ValueKind.ANY, f"No longer read: {_AGENT}.", status=FieldStatus.REMOVED, replacement=_AGENT),
)
_INVOCATION = Node(
    (
        Field("tool_name", _STRING, "The tool the agent is expected to call."),
        Field("tool_input", ValueKind.ANY, "The input the call is expected to pass."),
        Field("tool_output", ValueKind.ANY, "The output the call is expected to return."),
    ),
    open=True,
)
# A question's expectations; keys SST does not read are kept and sent with them.
_GROUND_TRUTH = Node(
    (
        Field(
            "ground_truth_invocations",
            _LIST,
            "The tool calls the answer is expected to make; an empty list expects none.",
            items=_INVOCATION,
        ),
        Field("ground_truth_output", _STRING, "The answer expected."),
        Field("required_filters", _LIST, "Filters the generated query must apply.", items=_STRING),
        Field("immutable", _BOOLEAN, "The expected answer does not change as data arrives."),
        Field("immutable_reason", _STRING, "Why the expected answer does not change."),
        Field(
            "ground_truth_subagents",
            _LIST,
            "The subagents a CoWork profile is expected to route to, each an `agent()` or `tool()` call.",
            items=_TEMPLATE,
        ),
    ),
    open=True,
)
_QUESTION = Node(
    (
        Field("question", _STRING, "The question put to the agent.", required=True),
        Field(
            "ground_truth", _MAPPING, "What the answer is expected to do and say.", required=True, node=_GROUND_TRUTH
        ),
    )
)
EVAL_DATASET = FileKind(
    "eval_dataset",
    "An eval dataset: questions, each with what its answer is expected to do and say.",
    f"a YAML file with `questions:` in {_EVALS}",
    Node(
        (
            Field("name", _STRING, "The dataset's name, which an agent's `evals.dataset` references.", required=True),
            Field("description", _STRING, "What the dataset covers."),
            Field("questions", _LIST, "The questions, in order.", required=True, items=_QUESTION),
            *_REFUSED,
        )
    ),
)
_SYSTEM_METRIC = Node(
    (
        Field(
            "name",
            ValueKind.ENUM,
            "The Snowflake metric to score.",
            choices=tuple(sorted(SYSTEM_EVAL_METRICS)),
        ),
        Field("version", _STRING, "The metric's version."),
        Field("gate", _BOOLEAN, "Fails the eval when the metric's score falls outside its threshold."),
        Field("threshold", _MAPPING, "The pass band of the metric's score.", node=_THRESHOLD),
        Field("judge_model", _STRING, "Read only to report that Snowflake chooses the judge model."),
    )
)
_ROUTING_METRIC = Node(
    (
        Field(
            "name",
            ValueKind.ENUM,
            "The routing metric to score.",
            required=True,
            choices=tuple(sorted(ROUTING_EVAL_METRICS)),
        ),
        Field("gate", _BOOLEAN, "Fails the eval when the metric's score falls outside its threshold."),
        Field("threshold", _MAPPING, "The pass band of the metric's score.", node=_THRESHOLD),
    )
)
_DATASET_CONFIG = Node(
    (
        Field(
            "mint",
            ValueKind.ENUM,
            "Whether SST creates the Snowflake dataset from the source table.",
            choices=tuple(sorted(EVAL_MINT_POLICIES)),
        ),
        Field("name_template", _STRING, "The name of the Snowflake dataset the eval runs on."),
        Field("source_table_template", _STRING, "The table the dataset's rows are written to."),
        Field(
            "column_mapping",
            _MAPPING,
            "The source table's question and expected-answer columns.",
            node=Node(
                (
                    Field("query_text", _STRING, "The column holding each question.", loose=True),
                    Field("ground_truth", _STRING, "The column holding each expected answer.", loose=True),
                )
            ),
        ),
    )
)
_RUN = Node(
    (
        Field("name_template", _STRING, "The run's name, which carries `{{ sha7 }}` so each commit's run is its own."),
        Field("label", _STRING, "A label shown with the run."),
        Field("description", _STRING, "What the run checks."),
        Field("variant", _STRING, "Which variant of the agent the run measures."),
        Field("accept_statuses", _LIST, "The run statuses that count as finished.", items=_STRING),
        _moved("tier", "an eval config sets it under config:, as config.eval_tier"),
        _moved("retry", "an eval config sets it under config:, as config.retry"),
        _moved("concurrency", "an eval config sets it under config:, as config.concurrency"),
        _moved("baseline_runs", "an eval config sets it under config:, as config.baseline_runs"),
        _moved("retention", "an eval config sets its retention class under config:, as config.retention"),
    )
)
EVAL_CONFIG = FileKind(
    "eval_config",
    "An eval config: the dataset an eval runs on, the metrics it scores, and how it runs.",
    f"a YAML file without `questions:` in {_EVALS}",
    Node(
        (
            Field("name", _STRING, "The config's name, which an agent's `evals.config` references.", required=True),
            Field("config", _MAPPING, "The eval's own `evals:` settings.", config_block="evals"),
            Field("dataset", _MAPPING, "The Snowflake dataset the eval runs on.", node=_DATASET_CONFIG),
            Field(
                "metrics",
                _MAPPING,
                "The metrics the eval scores.",
                required=True,
                node=Node(
                    (
                        Field("system", _LIST, "Snowflake's metrics, each with its gate.", items=_SYSTEM_METRIC),
                        Field("custom", _LIST, "Custom metrics, each an `eval_metric()` call.", items=_TEMPLATE),
                        Field(
                            "routing",
                            _LIST,
                            "A CoWork profile's routing metrics, each with its gate.",
                            items=_ROUTING_METRIC,
                        ),
                    )
                ),
            ),
            Field("run", _MAPPING, "How the run is named and which statuses finish it.", node=_RUN),
            Field(
                "sweep",
                ValueKind.ANY,
                "Refused: a sweep over agent variants is not supported.",
                status=FieldStatus.REFUSED,
                code="SST-CFG044",
            ),
            _moved("agent_version", "an eval config sets it under config:, as config.agent_version"),
            *_REFUSED,
        )
    ),
)
