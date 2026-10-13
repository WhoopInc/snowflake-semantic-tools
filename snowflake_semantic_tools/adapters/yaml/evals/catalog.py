"""Load every agent's eval files, and the custom metrics they reference, into one catalog.

An agent's `evals:` block names its dataset and its config with `eval_dataset()` and
`eval_config()`, by the `name:` each file declares. Every file in the agent's `evals/` folder
is read and checked -- one with `questions:` is a dataset, any other a config -- and only the
pair the agent names is published, so older versions can stay beside it.

The custom metrics load first, then each agent's eval files in agent order; the static
validator runs last, over the whole catalog, so its diagnostics follow every parse diagnostic.
Each file the agent does not name is then checked alone, paired with the one it names of the
other kind, and only what is found in that file is kept.

The `evals:` defaults that stand in for an unset value are filled in before validation, so the
validator, the renderer and the gate all read the same effective value: `+judge_model` for a
custom metric's absent `model`, `+metrics` for a config's absent `metrics.system`, and
`+metric_version` for a system metric's absent `version`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import TypeVar

from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.evals.config import parse_config
from snowflake_semantic_tools.adapters.yaml.evals.dataset import parse_dataset
from snowflake_semantic_tools.adapters.yaml.evals.metrics import load_custom_metrics
from snowflake_semantic_tools.adapters.yaml.evals.readers import origin_at
from snowflake_semantic_tools.adapters.yaml.fields import refused
from snowflake_semantic_tools.adapters.yaml.parse import read_yaml_file
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDefaults,
    EvalSystemMetric,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog

# An eval file the agent does not publish, paired for checking, and the file whose findings count.
_Candidate = tuple[ResolvedEval, str]
# A config file, parsed, with the file it was parsed from, which the defaults read again.
_Loaded = tuple[EvalConfig, tuple[str, ParsedYaml]]
_T = TypeVar("_T")


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

    An agent whose named dataset or config is missing, unreadable or ambiguous is left out. A custom metric reference
    that names no loaded metric is reported and dropped, and the eval kept; names match
    casefolded, and of two metrics sharing one, the later file's wins. A custom metric with no
    `model` takes `evals.+judge_model`; a config whose `metrics:` block has no `system` key
    enables, ungated, each system metric `evals.+metrics` lists; and a system metric with no
    `version` takes `evals.+metric_version`. `defaults`, `agent_tool_names` and
    `allowed_models` are for `validate_eval_catalog`, which runs last and reports the codes it
    lists.

    Args:
        initial_diagnostics: Diagnostics to place ahead of the loader's own, such as those of
            loading the agents and the defaults.

    Returns:
        The catalog; its diagnostics are `initial_diagnostics`, the parse diagnostics in load
        order, then the validator's.

    Diagnostics:
        SST-PRT009: an eval file, or the agent's `evals/` folder, is a symbolic link; it is
            not read.
        SST-REF048: `eval_dataset()` or `eval_config()` names no file of that kind in the
            agent's `evals/` folder.
        SST-VAL765: two dataset files, or two config files, in one folder share a name.
        SST-LOD018: an eval file or a metric file cannot be read.
        SST-LOD006: an eval file or a metric file is not UTF-8.
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
    metrics = tuple(
        replace(metric, model=defaults.judge_model) if metric.model is None else metric
        for metric in load_custom_metrics(project_dir, eval_metrics_dir, diagnostics)
    )
    metric_index = {metric.name.casefold(): metric for metric in metrics}
    evals: list[ResolvedEval] = []
    candidates: list[_Candidate] = []
    for agent in agents:
        resolved, unpublished = _load_eval(project_dir, agent, metric_index, defaults, diagnostics)
        if resolved is not None:
            evals.append(resolved)
        candidates.extend(unpublished)
    catalog = EvalCatalog(tuple(evals), metrics, defaults, DiagnosticBag(tuple(diagnostics)))
    validated = validate_eval_catalog(catalog, agent_tool_names=agent_tool_names, allowed_models=allowed_models)
    unpublished_findings = tuple(
        item
        for candidate, own_file in candidates
        for item in validate_eval_catalog(
            EvalCatalog((candidate,), metrics, defaults),
            agent_tool_names=agent_tool_names,
            allowed_models=allowed_models,
        )
        if item.origin is not None and item.origin.file == own_file
    )
    return EvalCatalog(
        catalog.evals, catalog.metrics, catalog.defaults, DiagnosticBag((*validated, *unpublished_findings))
    )


def _load_eval(
    project_dir: Path,
    agent: AgentModel,
    metric_index: Mapping[str, CustomEvalMetric],
    defaults: EvalDefaults,
    diagnostics: list[Diagnostic],
) -> tuple[ResolvedEval | None, tuple[_Candidate, ...]]:
    """Read every eval file in one agent's `evals/` folder; return its published eval and the rest.

    Returns:
        The eval of the dataset and config the agent names, None when it has no `evals:` or
        either does not resolve; and each other file, paired for checking with the file of the
        other kind the agent names, when that one resolved.
    """
    files = agent.evals
    if files is None:
        return None, ()
    subject = artifact_key("eval", agent.name.casefold())
    datasets, configs = _folder_files(project_dir, files.folder, diagnostics)
    dataset_index = _index(
        datasets, [(item.name, item.source_file) for item in datasets], "dataset", files.folder, subject, diagnostics
    )
    config_index = _index(
        configs, [(item.name, item.source_file) for item, _ in configs], "config", files.folder, subject, diagnostics
    )
    dataset = _named(dataset_index, files.dataset, "eval_dataset", agent, diagnostics)
    loaded = _named(config_index, files.config, "eval_config", agent, diagnostics)
    config = _with_system_defaults(*loaded, defaults) if loaded is not None else None
    candidates: list[_Candidate] = []
    if config is not None:
        candidates.extend(
            (ResolvedEval(agent, other, config, ()), other.source_file) for other in datasets if other is not dataset
        )
    if dataset is not None:
        for other in configs:
            if other is loaded:
                continue
            other_config = _with_system_defaults(*other, defaults)
            metrics = _resolve_metrics(agent, other_config, metric_index, diagnostics)
            candidates.append((ResolvedEval(agent, dataset, other_config, metrics), other_config.source_file))
    if dataset is None or config is None:
        return None, tuple(candidates)
    resolved = ResolvedEval(agent, dataset, config, _resolve_metrics(agent, config, metric_index, diagnostics))
    return resolved, tuple(candidates)


def _folder_files(
    project_dir: Path, folder: str, diagnostics: list[Diagnostic]
) -> tuple[list[EvalDataset], list[_Loaded]]:
    """Parse each `.yml` or `.yaml` file directly in `folder`, in sorted path order.

    A file with a `questions:` key is a dataset and any other a config. A file that cannot be
    read or parsed is reported and left out.

    Diagnostics:
        SST-PRT009: a file, or the folder, is a symbolic link; it is not read.
        Each code `read_yaml_file`, `parse_dataset` and `parse_config` list.
    """
    root = project_dir / folder
    if not root.is_dir() or refused(project_dir, root, diagnostics):
        return [], []
    datasets: list[EvalDataset] = []
    configs: list[_Loaded] = []
    for path in sorted(root.iterdir()):
        if path.suffix.casefold() not in (".yml", ".yaml") or refused(project_dir, path, diagnostics):
            continue
        relative = f"{folder}/{path.name}"
        parsed = read_yaml_file(path, relative, diagnostics)
        if parsed is None:
            continue
        if "questions" in parsed.tree:
            datasets.append(parse_dataset((relative, parsed), diagnostics))
        else:
            configs.append((parse_config((relative, parsed), diagnostics), (relative, parsed)))
    return datasets, configs


def _index(
    items: list[_T],
    names: list[tuple[str | None, str]],
    kind: str,
    folder: str,
    subject: str,
    diagnostics: list[Diagnostic],
) -> dict[str, _T]:
    """Index one kind's files by casefolded `name:`, given each one's name and file; the first is kept.

    Diagnostics:
        SST-VAL765: a later file declares a name an earlier one of its kind declared.
    """
    index: dict[str, _T] = {}
    files: dict[str, str] = {}
    for item, (name, source_file) in zip(items, names, strict=True):
        if name is None:
            continue
        key = name.casefold()
        if key in index:
            diagnostics.append(
                D(
                    "SST-VAL765",
                    path=folder,
                    kind=kind,
                    a=files[key],
                    b=source_file,
                    name=name,
                    origin=Origin(source_file),
                    subject=subject,
                )
            )
            continue
        index[key] = item
        files[key] = source_file
    return index


def _named(
    index: Mapping[str, _T], name: str | None, function: str, agent: AgentModel, diagnostics: list[Diagnostic]
) -> _T | None:
    """Return the file `function('<name>')` names; None when the agent named none or no file has it.

    Diagnostics:
        SST-REF048: no file of the function's kind in the agent's `evals/` folder has the name.
    """
    if name is None or agent.evals is None:
        return None
    found = index.get(name.casefold())
    if found is None:
        diagnostics.append(
            D(
                "SST-REF048",
                artifact=agent.origin.file,
                function=function,
                name=name,
                kind=function.removeprefix("eval_"),
                path=agent.evals.folder,
                origin=agent.evals.origin,
                subject=artifact_key("eval", agent.name.casefold()),
            )
        )
    return found


def _with_system_defaults(config: EvalConfig, loaded: tuple[str, ParsedYaml], defaults: EvalDefaults) -> EvalConfig:
    """Fill a config's system metrics from `evals.+metrics`, and their versions from `+metric_version`.

    The metrics `+metrics` lists stand in only when the `metrics:` block is a mapping with no
    `system` key: an explicit `system: []` enables none. Each is ungated, with no threshold,
    and placed at the `metrics:` block. Only a version the metric does not set is filled.
    """
    source_file, parsed = loaded
    block = parsed.tree.get("metrics")
    system = config.system_metrics
    if isinstance(block, dict) and "system" not in block:
        origin = origin_at(parsed, ("metrics",), source_file)
        system = tuple(EvalSystemMetric(origin, name) for name in defaults.metrics)
    return replace(
        config,
        system_metrics=tuple(
            replace(metric, version=defaults.metric_version) if metric.version is None else metric for metric in system
        ),
    )


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
