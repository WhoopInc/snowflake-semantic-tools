"""Load every agent's eval files, and the custom metrics they reference, into one catalog.

The custom metrics load first, then each agent's dataset and config in agent order; the
static validator runs last, over the whole catalog, so its diagnostics follow every parse
diagnostic.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.evals.config import parse_config
from snowflake_semantic_tools.adapters.yaml.evals.dataset import parse_dataset
from snowflake_semantic_tools.adapters.yaml.evals.metrics import load_custom_metrics
from snowflake_semantic_tools.adapters.yaml.parse import read_yaml_file
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDefaults,
    ResolvedEval,
    validate_eval_catalog,
)


def load_eval_catalog(
    project_dir: Path,
    agents: tuple[AgentModel, ...],
    *,
    eval_metrics_dir: str = "eval_metrics",
    defaults: EvalDefaults = EvalDefaults(),
    initial_diagnostics: DiagnosticBag = DiagnosticBag(),
    agent_tool_names: Mapping[str, tuple[str, ...]] | None = None,
    allowed_models: tuple[str, ...] = (),
) -> EvalCatalog:
    """Load the evals of `agents` from the files their `evals:` blocks name, then validate them.

    An agent whose dataset or config cannot be read is left out. A custom metric reference
    that names no loaded metric is reported and dropped, and the eval kept; names match
    casefolded, and of two metrics sharing one, the later file's wins. `defaults`,
    `agent_tool_names` and `allowed_models` are for `validate_eval_catalog`, which runs last
    and reports the codes it lists.

    Args:
        initial_diagnostics: Diagnostics to place ahead of the loader's own, such as those of
            loading the agents and the defaults.

    Returns:
        The catalog; its diagnostics are `initial_diagnostics`, the parse diagnostics in load
        order, then the validator's.

    Diagnostics:
        SST-LOD018: an eval file or a metric file cannot be read.
        SST-PRS122: an eval file or a metric file is not UTF-8.
        SST-LOD001: an eval file or a metric file is not valid YAML.
        SST-LOD002: an eval file or a metric file's root is not a mapping.
        SST-LOD003: an eval file or a metric file holds only whitespace or comments.
        SST-LOD004: a template is unterminated or nested, or a `metrics.custom` entry is not one
            `eval_metric()` reference.
        SST-LOD005: an eval file or a metric file writes a key twice in one mapping.
        SST-LOD008: an eval file or a metric file holds more than one document.
        SST-PRS002: a required field is absent.
        SST-PRS003: a field has the wrong type.
        SST-PRS004: a custom metric sets a key SST does not read.
        SST-PRS013: a config's `run.tier` or an accepted status is not an accepted value.
        SST-PRS018: a question, an expected invocation, or a metric entry has the wrong type.
        SST-PRS019: a boolean field is not a boolean.
        SST-REF026: a config references a custom metric that no metric file defines.
    """
    diagnostics: list[Diagnostic] = list(initial_diagnostics)
    metrics = load_custom_metrics(project_dir, eval_metrics_dir, diagnostics)
    metric_index = {metric.name.casefold(): metric for metric in metrics}
    evals: list[ResolvedEval] = []
    for agent in agents:
        resolved = _load_eval(project_dir, agent, metric_index, diagnostics)
        if resolved is not None:
            evals.append(resolved)
    catalog = EvalCatalog(tuple(evals), metrics, defaults, DiagnosticBag(tuple(diagnostics)))
    return EvalCatalog(
        catalog.evals,
        catalog.metrics,
        catalog.defaults,
        validate_eval_catalog(catalog, agent_tool_names=agent_tool_names, allowed_models=allowed_models),
    )


def _load_eval(
    project_dir: Path,
    agent: AgentModel,
    metric_index: Mapping[str, CustomEvalMetric],
    diagnostics: list[Diagnostic],
) -> ResolvedEval | None:
    """Read and parse one agent's eval files, or return None when it has none or one is unreadable.

    Both files are read before either is parsed, so each unreadable one is reported.
    """
    if agent.evals is None:
        return None
    dataset_parsed = _read_pointer(project_dir, agent.evals.dataset, agent.origin, diagnostics)
    config_parsed = _read_pointer(project_dir, agent.evals.config, agent.origin, diagnostics)
    if dataset_parsed is None or config_parsed is None:
        return None
    dataset = parse_dataset(dataset_parsed, diagnostics)
    config = parse_config(config_parsed, diagnostics)
    return ResolvedEval(agent, dataset, config, _resolve_metrics(agent, config, metric_index, diagnostics))


def _resolve_metrics(
    agent: AgentModel,
    config: EvalConfig,
    metric_index: Mapping[str, CustomEvalMetric],
    diagnostics: list[Diagnostic],
) -> tuple[CustomEvalMetric, ...]:
    """Resolve a config's custom metric names, casefolded; a name no metric has is dropped.

    Diagnostics:
        SST-REF026: a name that no loaded custom metric has.
    """
    resolved_metrics: list[CustomEvalMetric] = []
    for name in config.custom_metric_names:
        metric = metric_index.get(name.casefold())
        if metric is None:
            diagnostics.append(
                D(
                    "SST-REF026",
                    name=name,
                    origin=config.origin,
                    subject=artifact_key("eval", agent.name.casefold()),
                )
            )
        else:
            resolved_metrics.append(metric)
    return tuple(resolved_metrics)


def _read_pointer(
    project_dir: Path,
    relative: str | None,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> tuple[str, ParsedYaml] | None:
    if relative is None:
        return None
    parsed = read_yaml_file(project_dir / relative, relative, diagnostics, pointer_origin=origin)
    return None if parsed is None else (relative, parsed)
