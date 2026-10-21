"""Load every agent's and CoWork profile's eval files, and the custom metrics they reference.

An agent's `evals:` block names its dataset and its config with `eval_dataset()` and
`eval_config()`, by the `name:` each file declares. Every file in the agent's `evals/` folder
is read and checked -- one with `questions:` is a dataset, any other a config -- and only the
pair the agent names is published, so older versions can stay beside it. A CoWork profile's
`evals:` block is read the same way, from the `evals/` folder beside its `cowork_profile.yml`;
CoWork profiles are a Snowflake private preview (PrPr) feature:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

The custom metrics load first, then each agent's eval files in agent order, then each
profile's; the static validator runs last, over the whole catalog, so its diagnostics follow
every parse diagnostic. Each file the subject does not name is then checked alone, paired with
the one it names of the other kind, and only what is found in that file is kept.

Each eval resolves its `evals:` settings -- its config's own `config:`, then the folder routes
over its subject's folder, then the block root -- and the ones that stand in for an unset value
are filled in before validation, so the validator, the renderer and the gate all read the same
value: `judge_model` for a custom metric's absent `model`, `metrics` for a config's absent
`metrics.system`, and `metric_version` for a system metric's absent `version`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields, replace
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
from snowflake_semantic_tools.domain.model.config_schema import resolve_settings
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDefaults,
    EvalSubject,
    EvalSystemMetric,
    ResolvedEval,
    eval_defaults,
)
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog

# An eval file the agent does not publish, paired for checking, and the file whose findings count.
_Candidate = tuple[ResolvedEval, str]
# A config file, parsed, with the file it was parsed from, which the defaults read again.
_Loaded = tuple[EvalConfig, tuple[str, ParsedYaml]]
# The `evals:` block as parsed, which each eval resolves its settings over, and its root's
# settings, which stand in for any the block does not resolve.
_Root = tuple[object, EvalDefaults]
_T = TypeVar("_T")


def load_eval_catalog(
    project_dir: Path,
    agents: tuple[AgentModel, ...],
    *,
    eval_metrics_dir: str = "eval_metrics",
    defaults: EvalDefaults = EvalDefaults(),
    block: object = None,
    initial_diagnostics: DiagnosticBag = DiagnosticBag(),
    agent_tool_names: Mapping[str, tuple[str, ...]] | None = None,
    allowed_models: tuple[str, ...] = (),
    cowork_profiles: tuple[CoworkProfileModel, ...] = (),
) -> EvalCatalog:
    """Load the evals of `agents` and `cowork_profiles` from the files their `evals:` blocks name.

    The catalog is then validated. A subject whose named dataset or config is missing,
    unreadable or ambiguous is left out. A custom metric reference
    that names no loaded metric is reported and dropped, and the eval kept; names match
    casefolded, and of two metrics sharing one, the later file's wins. A custom metric with no
    `model` takes its eval's `judge_model` setting, and in the catalog's own list the root's;
    a config whose `metrics:` block has no `system` key enables, ungated, each system metric
    its `metrics` setting lists; and a system metric with no `version` takes its eval's
    `metric_version`. `defaults`, `agent_tool_names` and `allowed_models` are for
    `validate_eval_catalog`, which runs last and reports the codes it lists.

    Args:
        defaults: The `evals:` block root's settings.
        block: The `evals:` block as parsed, whose root and folder routes each eval's settings
            resolve through, below its config's `config:`; None resolves them from `config:`
            alone.
        initial_diagnostics: Diagnostics to place ahead of the loader's own, such as those of
            loading the agents and the defaults.
        cowork_profiles: The CoWork profiles whose evals load after the agents'. Snowflake
            private preview (PrPr).

    Returns:
        The catalog; its diagnostics are `initial_diagnostics`, the parse diagnostics in load
        order, then the validator's.

    Diagnostics:
        SST-PRT009: an eval file, or the agent's `evals/` folder, is a symbolic link; it is
            not read.
        SST-REF048: `eval_dataset()` or `eval_config()` names no file of that kind in the
            subject's `evals/` folder.
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
        SST-PRS013: a config's accepted status, or a `config.metrics` name, is not an accepted value.
        SST-PRS126: a config writes `agent_version`, or a `run:` setting, outside `config:`.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports a config's `config:` block.
        SST-PRS018: a question, an expected invocation, or a metric entry has the wrong type.
        SST-PRS019: a boolean field is not a boolean.
        SST-REF026: a config references a custom metric that no metric file defines.
    """
    diagnostics: list[Diagnostic] = list(initial_diagnostics)
    authored = load_custom_metrics(project_dir, eval_metrics_dir, diagnostics)
    metrics = _judged(authored, defaults.judge_model)
    metric_index = {metric.name.casefold(): metric for metric in authored}
    evals: list[ResolvedEval] = []
    candidates: list[_Candidate] = []
    subjects = (*(EvalSubject(agent) for agent in agents), *(EvalSubject(profile) for profile in cowork_profiles))
    for subject in subjects:
        resolved, unpublished = _load_eval(project_dir, subject, metric_index, (block, defaults), diagnostics)
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


def _judged(metrics: tuple[CustomEvalMetric, ...], judge_model: str | None) -> tuple[CustomEvalMetric, ...]:
    """Give each custom metric that names no `model` the judge model a setting resolved."""
    return tuple(replace(metric, model=judge_model) if metric.model is None else metric for metric in metrics)


def _load_eval(
    project_dir: Path,
    subject: EvalSubject,
    metric_index: Mapping[str, CustomEvalMetric],
    root: _Root,
    diagnostics: list[Diagnostic],
) -> tuple[ResolvedEval | None, tuple[_Candidate, ...]]:
    """Read every eval file in one subject's `evals/` folder; return its published eval and the rest.

    Each config's settings resolve over the `evals:` block along the subject's folder, below its
    own `config:`, as `_settled` does.

    Returns:
        The eval of the dataset and config the subject names, None when it has no `evals:` or
        either does not resolve; and each other file, paired for checking with the file of the
        other kind the subject names, when that one resolved.
    """
    files = subject.evals
    if files is None:
        return None, ()
    key = subject.eval_key
    datasets, configs = _folder_files(project_dir, files.folder, diagnostics)
    dataset_index = _index(
        datasets, [(item.name, item.source_file) for item in datasets], "dataset", files.folder, key, diagnostics
    )
    config_index = _index(
        configs, [(item.name, item.source_file) for item, _ in configs], "config", files.folder, key, diagnostics
    )
    dataset = _named(dataset_index, files.dataset, "eval_dataset", subject, diagnostics)
    loaded = _named(config_index, files.config, "eval_config", subject, diagnostics)
    candidates: list[_Candidate] = []
    if loaded is not None:
        bare = _settled(subject, loaded, root)
        candidates.extend(
            (replace(bare, dataset=other), other.source_file) for other in datasets if other is not dataset
        )
    if dataset is not None:
        for other in configs:
            if other is loaded:
                continue
            candidate = _settled(subject, other, root, dataset)
            metrics = _resolve_metrics(candidate, metric_index, root, diagnostics)
            candidates.append((replace(candidate, custom_metrics=metrics), candidate.config.source_file))
    if dataset is None or loaded is None:
        return None, tuple(candidates)
    resolved = _settled(subject, loaded, root, dataset)
    return replace(resolved, custom_metrics=_resolve_metrics(resolved, metric_index, root, diagnostics)), tuple(
        candidates
    )


def _settled(subject: EvalSubject, loaded: _Loaded, root: _Root, dataset: EvalDataset | None = None) -> ResolvedEval:
    """Pair a config with the subject and a dataset, resolve its settings, and fill its system metrics.

    Without a dataset, the pair is the config's alone, until the caller gives it one; it names no
    custom metric until the caller resolves them.
    """
    config, source = loaded
    settings = resolve_settings(root[0], subject.folder, config.config)
    filled = _with_system_defaults(config, source, _options(eval_defaults(settings), root))
    placeholder = dataset or EvalDataset(config.origin, config.source_file, None, ())
    return ResolvedEval(subject.model, placeholder, filled, (), settings)


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
    index: Mapping[str, _T], name: str | None, function: str, subject: EvalSubject, diagnostics: list[Diagnostic]
) -> _T | None:
    """Return the file `function('<name>')` names; None when the subject named none or no file has it.

    Diagnostics:
        SST-REF048: no file of the function's kind in the subject's `evals/` folder has the name.
    """
    files = subject.evals
    if name is None or files is None:
        return None
    found = index.get(name.casefold())
    if found is None:
        diagnostics.append(
            D(
                "SST-REF048",
                artifact=subject.origin.file,
                function=function,
                name=name,
                kind=function.removeprefix("eval_"),
                path=files.folder,
                origin=files.origin,
                subject=subject.eval_key,
            )
        )
    return found


def _with_system_defaults(config: EvalConfig, loaded: tuple[str, ParsedYaml], defaults: EvalDefaults) -> EvalConfig:
    """Fill a config's system metrics from its `metrics` setting, and their versions from `metric_version`.

    The metrics the setting lists stand in only when the `metrics:` block is a mapping with no
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


def _options(resolved: EvalDefaults, root: _Root) -> EvalDefaults:
    """Return the settings an eval resolved, each one it left unset taken from the root's defaults."""
    values = {item.name: getattr(resolved, item.name) for item in fields(EvalDefaults)}
    return replace(root[1], **{name: value for name, value in values.items() if value is not None and value != ()})


def _resolve_metrics(
    resolved: ResolvedEval,
    metric_index: Mapping[str, CustomEvalMetric],
    root: _Root,
    diagnostics: list[Diagnostic],
) -> tuple[CustomEvalMetric, ...]:
    """Resolve a config's custom metric names, casefolded; a name no metric has is dropped.

    A metric that names no `model` takes the eval's `judge_model` setting, else the root's.

    Diagnostics:
        SST-REF026: a name that no loaded custom metric has.
    """
    config = resolved.config
    resolved_metrics: list[CustomEvalMetric] = []
    for name in config.custom_metric_names:
        metric = metric_index.get(name.casefold())
        if metric is None:
            diagnostics.append(
                D(
                    "SST-REF026",
                    name=name,
                    origin=config.origin,
                    subject=resolved.key,
                )
            )
        else:
            resolved_metrics.append(metric)
    return _judged(tuple(resolved_metrics), _options(resolved.options, root).judge_model)
