"""A valid eval catalog and the edits that break one rule of it, for the per-code eval tests.

`sales_eval` is one agent, `sales`, with an Analyst tool over `SALES_VIEW` and `web_search`;
one question that expects the Analyst tool; a gated `tool_selection_accuracy`; a run block;
and the custom metric `grounding`, which validates clean. `validate` checks a catalog the
way compile does, with that agent's tool names and the judge model allowed. `only` returns
the single diagnostic a test expects, failing on none or several.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
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
    EvalRunConfig,
    EvalScoreRanges,
    EvalSystemMetric,
    ResolvedEval,
    ThresholdRange,
)
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
    value = EvalDataset(Origin(DATASET_FILE), DATASET_FILE, "sales", None, questions or (question(),))
    return replace(value, **changes)  # type: ignore[arg-type]


def system_metric(name: str = "tool_selection_accuracy", **changes: object) -> EvalSystemMetric:
    value = EvalSystemMetric(Origin(CONFIG_FILE, 10), name, "v3", True, ThresholdRange(min=0.8))
    return replace(value, **changes)  # type: ignore[arg-type]


def run_config(**changes: object) -> EvalRunConfig:
    value = EvalRunConfig(
        "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}",
        variant="ci",
        retry=1,
        concurrency=2,
        baseline_runs=5,
        accept_statuses=("COMPLETED",),
    )
    return replace(value, **changes)  # type: ignore[arg-type]


def config(**changes: object) -> EvalConfig:
    value = EvalConfig(
        Origin(CONFIG_FILE),
        CONFIG_FILE,
        "sales",
        "committed",
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
) -> ResolvedEval:
    return ResolvedEval(
        agent or sales_agent(),
        eval_dataset or dataset(),
        eval_config or config(),
        (judge(),) if metrics is None else metrics,
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


def only(diagnostics: Iterable[Diagnostic], code: str) -> Diagnostic:
    found = [item for item in diagnostics if item.code == code]
    assert len(found) == 1, f"expected one {code}, found {[item.message for item in found]}"
    return found[0]


def codes(diagnostics: Iterable[Diagnostic]) -> list[str]:
    return [item.code for item in diagnostics]
