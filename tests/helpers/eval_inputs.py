"""A valid eval catalog and the edits that break one rule of it, for the per-code eval tests.

`sales_eval` is one agent, `sales`, with an Analyst tool over `SALES_VIEW` and `web_search`;
one question that expects the Analyst tool; a gated `tool_selection_accuracy`; a run block;
the custom metric `grounding`, which validates clean; and the settings `SETTINGS`, as its
config's own `config:` would set them. `settled` gives an eval more settings. `validate`
checks a catalog the way compile does, with that agent's tool names and the judge model
model allowed. `only` returns the single diagnostic a test expects, failing on none or several.

`concierge_eval` is a CoWork profile's eval (a Snowflake private preview): the profile
`concierge` routes to `agent('sales')` and `tool('partner_agent')`, and its dataset has one row
routed to sales and one decline, scored by a gated `routing_accuracy` and an ungated
`answer_correctness`. `routed` builds one of its rows.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel, CoworkSubagent
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDatasetConfig,
    EvalDefaults,
    EvalGroundTruth,
    EvalInvocation,
    EvalQuestion,
    EvalRoutingMetric,
    EvalRunConfig,
    EvalScoreRanges,
    EvalSubagentRef,
    EvalSystemMetric,
    ResolvedEval,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.setting import CONFIG_SOURCE, Setting
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog

AGENT_FILE = "agents/sales/agent.yml"
DATASET_FILE = "agents/sales/evals/dataset.yml"
CONFIG_FILE = "agents/sales/evals/config.yml"
JUDGE = "claude-sonnet-4-6"
TOOL_NAMES = ("SALES_VIEW", "web_search")
JUDGE_PROMPT = (
    "Score from 0 to 5. If information is insufficient, score 0. If tied, choose the lower score. "
    "Return only the numeric score. {{input}} {{ground_truth}}"
)


def sales_agent(**changes: object) -> AgentModel:
    origin = Origin(AGENT_FILE)
    agent = AgentModel(
        "sales",
        origin,
        (AGENT_FILE,),
        sample_questions=("Show revenue by region.",),
        tools=(
            AgentTool("cortex_analyst_text_to_sql", origin, semantic_view="sales_view"),
            AgentTool("web_search", origin, name="web_search", description="Search the web."),
        ),
    )
    return replace(agent, **changes)  # type: ignore[arg-type]


def question(text: str | None = "Show revenue in calendar year 2025.", **truth: object) -> EvalQuestion:
    origin = Origin(DATASET_FILE, 4)
    values: dict[str, object] = {
        "invocations": (EvalInvocation(Origin(DATASET_FILE, 7), "SALES_VIEW"),),
        "output": "State the direction and cite the source.",
    }
    values.update(truth)
    return EvalQuestion(origin, text, EvalGroundTruth(Origin(DATASET_FILE, 6), **values))  # type: ignore[arg-type]


def dataset(*questions: EvalQuestion, **changes: object) -> EvalDataset:
    value = EvalDataset(Origin(DATASET_FILE), DATASET_FILE, None, questions or (question(),))
    return replace(value, **changes)  # type: ignore[arg-type]


def system_metric(name: str = "tool_selection_accuracy", **changes: object) -> EvalSystemMetric:
    value = EvalSystemMetric(Origin(CONFIG_FILE, 10), name, "v3", True, ThresholdRange(min=0.8))
    return replace(value, **changes)  # type: ignore[arg-type]


SETTINGS: Mapping[str, object] = MappingProxyType(
    {"agent_version": "committed", "retry": 1, "concurrency": 2, "baseline_runs": 5}
)


def settings_of(values: Mapping[str, object], source: str = CONFIG_SOURCE) -> Mapping[str, Setting]:
    """Resolved settings holding `values`, each as though one step of the chain set it."""
    return MappingProxyType({key: Setting(key, value, source) for key, value in values.items()})


def settled(resolved: ResolvedEval, source: str = CONFIG_SOURCE, **values: object) -> ResolvedEval:
    """The eval with `values` among its resolved settings; a value of None removes the key."""
    kept = {key: item for key, item in resolved.settings.items() if key not in values}
    added = {key: Setting(key, value, source) for key, value in values.items() if value is not None}
    return replace(resolved, settings=MappingProxyType({**kept, **added}))


def run_config(**changes: object) -> EvalRunConfig:
    value = EvalRunConfig(
        "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}",
        variant="ci",
        accept_statuses=("COMPLETED",),
    )
    return replace(value, **changes)  # type: ignore[arg-type]


def config(**changes: object) -> EvalConfig:
    value = EvalConfig(
        Origin(CONFIG_FILE),
        CONFIG_FILE,
        EvalDatasetConfig("auto", "EVAL_{{ agent | upper }}_{{ sha7 }}", "SRC_{{ agent }}_{{ sha7 }}"),
        (system_metric(),),
        ("grounding",),
        run_config(),
    )
    return replace(value, **changes)  # type: ignore[arg-type]


def judge(name: str = "grounding", **changes: object) -> CustomEvalMetric:
    value = CustomEvalMetric(
        Origin(f"eval_metrics/{name}.yml"),
        f"eval_metrics/{name}.yml",
        name,
        None,
        JUDGE,
        EvalScoreRanges((0, 1), (2, 3), (4, 5)),
        JUDGE_PROMPT,
        gate_default=True,
        threshold_default=ThresholdRange(min=3),
    )
    return replace(value, **changes)  # type: ignore[arg-type]


def sales_eval(
    *,
    agent: AgentModel | None = None,
    eval_dataset: EvalDataset | None = None,
    eval_config: EvalConfig | None = None,
    metrics: tuple[CustomEvalMetric, ...] | None = None,
    settings: Mapping[str, object] = SETTINGS,
) -> ResolvedEval:
    return ResolvedEval(
        agent or sales_agent(),
        eval_dataset or dataset(),
        eval_config or config(),
        (judge(),) if metrics is None else metrics,
        settings_of(settings),
    )


PROFILE_FILE = "cowork_profiles/concierge/cowork_profile.yml"
PROFILE_DATASET_FILE = "cowork_profiles/concierge/evals/dataset.yml"
PROFILE_CONFIG_FILE = "cowork_profiles/concierge/evals/config.yml"


def concierge_profile(**changes: object) -> CoworkProfileModel:
    origin = Origin(PROFILE_FILE)
    profile = CoworkProfileModel(
        "concierge",
        origin,
        (PROFILE_FILE,),
        sample_questions=("Who sold the most?",),
        subagents=(CoworkSubagent("agent", "sales", origin), CoworkSubagent("tool", "partner_agent", origin)),
    )
    return replace(profile, **changes)  # type: ignore[arg-type]


def routed(text: str, *expected: str, **truth: object) -> EvalQuestion:
    """A profile's row expecting the subagents `expected`, each `<function>:<name>`; none is a decline."""
    origin = Origin(PROFILE_DATASET_FILE, 4)
    refs = tuple(EvalSubagentRef(Origin(PROFILE_DATASET_FILE, 7), *item.split(":", 1)) for item in expected)
    values: dict[str, object] = {"output": "State which agent answered.", "subagents": refs}
    values.update(truth)
    return EvalQuestion(origin, text, EvalGroundTruth(Origin(PROFILE_DATASET_FILE, 6), **values))  # type: ignore[arg-type]


def routing_metric(**changes: object) -> EvalRoutingMetric:
    value = EvalRoutingMetric(Origin(PROFILE_CONFIG_FILE, 12), "routing_accuracy", True, ThresholdRange(min=0.9))
    return replace(value, **changes)  # type: ignore[arg-type]


def concierge_eval(
    *questions: EvalQuestion,
    profile: CoworkProfileModel | None = None,
    **config_changes: object,
) -> ResolvedEval:
    rows = questions or (routed("Which region sold the most?", "agent:sales"), routed("Which supplier is late?"))
    values: dict[str, object] = {
        "origin": Origin(PROFILE_CONFIG_FILE),
        "source_file": PROFILE_CONFIG_FILE,
        "system_metrics": (system_metric("answer_correctness", gate=False, threshold=None),),
        "custom_metric_names": (),
        "routing_metrics": (routing_metric(),),
    }
    values.update(config_changes)
    return ResolvedEval(
        profile or concierge_profile(),
        EvalDataset(Origin(PROFILE_DATASET_FILE), PROFILE_DATASET_FILE, None, rows),
        config(**values),
        (),
        settings_of(SETTINGS),
    )


def validate(
    *evals: ResolvedEval,
    metrics: tuple[CustomEvalMetric, ...] | None = None,
    defaults: EvalDefaults = EvalDefaults(),
    tool_names: Iterable[str] = TOOL_NAMES,
) -> DiagnosticBag:
    values = evals or (sales_eval(),)
    custom = metrics if metrics is not None else tuple({m.name: m for e in values for m in e.custom_metrics}.values())
    return validate_eval_catalog(
        EvalCatalog(values, custom, defaults),
        agent_tool_names={"sales": tuple(tool_names)},
        allowed_models=(JUDGE,),
    )
