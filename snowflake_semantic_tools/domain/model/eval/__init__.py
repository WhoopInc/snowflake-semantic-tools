"""Cortex Agent evaluations: authoring values, name templates, and results.

`model` holds the authoring values and the eval contract's constants, `naming` the name
template grammar, and `results` the run, baseline and gate records. The static validator is
`domain.validate.eval`. This module re-exports the public names, so importers never reach
into the submodules.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.eval.model import (
    DEFAULT_EVAL_CONFIG_STAGE,
    EVAL_COMPLETED,
    EVAL_CONCURRENCY_MIN,
    EVAL_PASS_STATUSES,
    EVAL_RETRY_MIN,
    EVAL_TERMINAL_STATUSES,
    SUPPORTED_JUDGE_PLACEHOLDERS,
    SYSTEM_EVAL_METRIC_VERSION,
    SYSTEM_EVAL_METRICS,
    CustomEvalMetric,
    EvalCatalog,
    EvalColumnMapping,
    EvalConfig,
    EvalDataset,
    EvalDatasetConfig,
    EvalDefaults,
    EvalGroundTruth,
    EvalInvocation,
    EvalQuestion,
    EvalRetention,
    EvalRunConfig,
    EvalScoreRanges,
    EvalSweepConfig,
    EvalSystemMetric,
    ResolvedEval,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.eval.naming import render_eval_name_template
from snowflake_semantic_tools.domain.model.eval.results import (
    EvalBaselineMetric,
    EvalBaselineRecord,
    EvalCostSummary,
    EvalGateState,
    EvalGateVerdict,
    EvalMetricResult,
    EvalRegression,
    EvalResultRow,
    EvalRunAttempt,
)

__all__ = [
    "CustomEvalMetric",
    "DEFAULT_EVAL_CONFIG_STAGE",
    "EVAL_COMPLETED",
    "EVAL_CONCURRENCY_MIN",
    "EVAL_PASS_STATUSES",
    "EVAL_RETRY_MIN",
    "EVAL_TERMINAL_STATUSES",
    "EvalBaselineMetric",
    "EvalBaselineRecord",
    "EvalCatalog",
    "EvalColumnMapping",
    "EvalConfig",
    "EvalCostSummary",
    "EvalDataset",
    "EvalDatasetConfig",
    "EvalDefaults",
    "EvalGateState",
    "EvalGateVerdict",
    "EvalGroundTruth",
    "EvalInvocation",
    "EvalMetricResult",
    "EvalQuestion",
    "EvalRegression",
    "EvalResultRow",
    "EvalRetention",
    "EvalRunAttempt",
    "EvalRunConfig",
    "EvalScoreRanges",
    "EvalSweepConfig",
    "EvalSystemMetric",
    "ResolvedEval",
    "SUPPORTED_JUDGE_PLACEHOLDERS",
    "SYSTEM_EVAL_METRICS",
    "SYSTEM_EVAL_METRIC_VERSION",
    "ThresholdRange",
    "render_eval_name_template",
]
