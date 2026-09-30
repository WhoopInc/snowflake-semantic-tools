"""Parse Cortex Agent evaluation datasets, configs, and custom judges."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...domain.model.agent import AgentModel
from ...domain.model.artifact_key import artifact_key
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ...domain.model.eval import (
    EVAL_TERMINAL_STATUSES,
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
    validate_eval_catalog,
)
from ...domain.model.reference import TemplateSyntaxError, single_template_call
from .documents import ParsedYaml
from .fields import checked_strings, optional_int, optional_string, report_unknown_keys
from .parse import read_yaml_file

_GROUND_TRUTH_KEYS = frozenset(
    (
        "ground_truth_invocations",
        "ground_truth_output",
        "required_filters",
        "immutable",
        "immutable_reason",
    )
)
_CUSTOM_METRIC_KEYS = frozenset(
    (
        "name",
        "description",
        "model",
        "score_ranges",
        "prompt",
        "gate_default",
        "threshold_default",
        "enabled",
        "meta",
    )
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
    diagnostics: list[Diagnostic] = list(initial_diagnostics)
    metrics = _load_custom_metrics(project_dir, eval_metrics_dir, diagnostics)
    metric_index = {metric.name.casefold(): metric for metric in metrics}
    evals: list[ResolvedEval] = []
    for agent in agents:
        if agent.evals is None:
            continue
        dataset_parsed = _read_pointer(project_dir, agent.evals.dataset, agent.origin, diagnostics)
        config_parsed = _read_pointer(project_dir, agent.evals.config, agent.origin, diagnostics)
        if dataset_parsed is None or config_parsed is None:
            continue
        dataset = _parse_dataset(dataset_parsed, diagnostics)
        config = _parse_config(config_parsed, diagnostics)
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
        evals.append(ResolvedEval(agent, dataset, config, tuple(resolved_metrics)))
    catalog = EvalCatalog(tuple(evals), metrics, defaults, DiagnosticBag(tuple(diagnostics)))
    return EvalCatalog(
        catalog.evals,
        catalog.metrics,
        catalog.defaults,
        validate_eval_catalog(
            catalog,
            agent_tool_names=agent_tool_names,
            allowed_models=allowed_models,
        ),
    )


def parse_eval_defaults(value: object) -> tuple[EvalDefaults, DiagnosticBag]:
    if value is None:
        return EvalDefaults(), DiagnosticBag()
    if not isinstance(value, dict):
        return EvalDefaults(), DiagnosticBag(
            (D("SST-PRS003", artifact="sst_config.yml", field="evals", expected="mapping", found=type(value).__name__),)
        )
    diagnostics: list[Diagnostic] = []
    origin = Origin("sst_config.yml")
    metrics = _string_tuple(value.get("+metrics"), "evals.+metrics", origin, diagnostics)
    eval_tier = optional_string(value.get("+eval_tier"))
    if eval_tier is not None and eval_tier not in {"blocking", "report"}:
        diagnostics.append(
            D(
                "SST-PRS013",
                artifact="sst_config.yml",
                field="evals.+eval_tier",
                found=eval_tier,
                expected="blocking, report",
                origin=origin,
            )
        )
    return (
        EvalDefaults(
            eval_tier=eval_tier,
            metrics=metrics,
            metric_version=optional_string(value.get("+metric_version")),
            judge_model=optional_string(value.get("+judge_model")),
            agent_version=optional_string(value.get("+agent_version")),
            retry=optional_int(value.get("+retry")),
            concurrency=optional_int(value.get("+concurrency")),
            baseline_runs=optional_int(value.get("+baseline_runs")),
            min_dataset_rows=optional_int(value.get("+min_dataset_rows")),
            retention=optional_string(value.get("+retention")),
        ),
        DiagnosticBag(tuple(diagnostics)),
    )


def _load_custom_metrics(
    project_dir: Path,
    directory: str,
    diagnostics: list[Diagnostic],
) -> tuple[CustomEvalMetric, ...]:
    root = project_dir / directory
    if not root.is_dir():
        return ()
    metrics: list[CustomEvalMetric] = []
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.suffix.casefold() in (".yml", ".yaml")):
        relative = path.relative_to(project_dir).as_posix()
        parsed = read_yaml_file(path, relative, diagnostics)
        if parsed is None:
            continue
        metric = _parse_custom_metric(relative, parsed, diagnostics)
        if metric is not None:
            metrics.append(metric)
    return tuple(metrics)


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


def _parse_dataset(loaded: tuple[str, ParsedYaml], diagnostics: list[Diagnostic]) -> EvalDataset:
    source_file, parsed = loaded
    tree = parsed.tree
    origin = _origin(parsed, (), source_file)
    agent = _required_string(tree, "agent", source_file, parsed, diagnostics)
    description = _optional_string_field(tree, "description", source_file, parsed, diagnostics)
    questions_value = tree.get("questions")
    if questions_value is None:
        diagnostics.append(D("SST-PRS002", artifact=source_file, field="questions", origin=origin))
        questions: tuple[EvalQuestion, ...] = ()
    elif not isinstance(questions_value, list):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="questions",
                expected="list",
                found=type(questions_value).__name__,
                origin=_origin(parsed, ("questions",), source_file),
            )
        )
        questions = ()
    else:
        questions = tuple(
            _parse_question(source_file, parsed, index, value, diagnostics)
            for index, value in enumerate(questions_value)
        )
    return EvalDataset(
        origin,
        source_file,
        agent,
        description,
        questions,
        tuple(key for key in ("database", "schema", "enabled") if key in tree),
    )


def _parse_question(
    source_file: str,
    parsed: ParsedYaml,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalQuestion:
    origin = _origin(parsed, ("questions", index), source_file)
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                artifact=source_file,
                field="questions",
                index=index,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return EvalQuestion(origin, None, None)
    question = _optional_string_field(value, "question", source_file, parsed, diagnostics, ("questions", index))
    ground_truth_value = value.get("ground_truth")
    if not isinstance(ground_truth_value, dict):
        if ground_truth_value is None:
            diagnostics.append(
                D("SST-PRS002", artifact=source_file, field=f"questions[{index}].ground_truth", origin=origin)
            )
        else:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"questions[{index}].ground_truth",
                    expected="mapping",
                    found=type(ground_truth_value).__name__,
                    origin=origin,
                )
            )
        ground_truth = None
    else:
        ground_truth = _parse_ground_truth(source_file, parsed, index, ground_truth_value, diagnostics)
    return EvalQuestion(origin, question, ground_truth)


def _parse_ground_truth(
    source_file: str,
    parsed: ParsedYaml,
    question_index: int,
    value: Mapping[str, object],
    diagnostics: list[Diagnostic],
) -> EvalGroundTruth:
    path = ("questions", question_index, "ground_truth")
    origin = _origin(parsed, path, source_file)
    invocations: tuple[EvalInvocation, ...] | None = None
    if "ground_truth_invocations" in value:
        raw_invocations = value["ground_truth_invocations"]
        if not isinstance(raw_invocations, list):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"questions[{question_index}].ground_truth.ground_truth_invocations",
                    expected="list",
                    found=type(raw_invocations).__name__,
                    origin=origin,
                )
            )
        else:
            invocations = tuple(
                _parse_invocation(source_file, parsed, question_index, index, item, diagnostics)
                for index, item in enumerate(raw_invocations)
            )
    required_filters = _string_tuple(
        value.get("required_filters"),
        f"questions[{question_index}].ground_truth.required_filters",
        origin,
        diagnostics,
    )
    return EvalGroundTruth(
        origin=origin,
        invocations=invocations,
        output=_optional_string_field(value, "ground_truth_output", source_file, parsed, diagnostics, path),
        required_filters=required_filters,
        immutable=_optional_bool_field(value, "immutable", source_file, origin, diagnostics),
        immutable_reason=_optional_string_field(value, "immutable_reason", source_file, parsed, diagnostics, path),
        extra=MappingProxyType({str(key): item for key, item in value.items() if key not in _GROUND_TRUTH_KEYS}),
    )


def _parse_invocation(
    source_file: str,
    parsed: ParsedYaml,
    question_index: int,
    invocation_index: int,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalInvocation:
    path = ("questions", question_index, "ground_truth", "ground_truth_invocations", invocation_index)
    origin = _origin(parsed, path, source_file)
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                artifact=source_file,
                field=f"questions[{question_index}].ground_truth.ground_truth_invocations",
                index=invocation_index,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return EvalInvocation(origin)
    return EvalInvocation(
        origin,
        _optional_string_field(value, "tool_name", source_file, parsed, diagnostics, path),
        _optional_string_field(value, "tool_input", source_file, parsed, diagnostics, path),
        _optional_string_field(value, "tool_output", source_file, parsed, diagnostics, path),
    )


def _parse_config(loaded: tuple[str, ParsedYaml], diagnostics: list[Diagnostic]) -> EvalConfig:
    source_file, parsed = loaded
    tree = parsed.tree
    origin = _origin(parsed, (), source_file)
    agent = _required_string(tree, "agent", source_file, parsed, diagnostics)
    agent_version = _optional_string_field(tree, "agent_version", source_file, parsed, diagnostics)
    dataset = _parse_dataset_config(source_file, parsed, tree.get("dataset"), diagnostics)
    metrics = tree.get("metrics")
    system_metrics: tuple[EvalSystemMetric, ...] = ()
    custom_metric_names: tuple[str, ...] = ()
    if not isinstance(metrics, dict):
        if metrics is None:
            diagnostics.append(D("SST-PRS002", artifact=source_file, field="metrics", origin=origin))
        else:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field="metrics",
                    expected="mapping",
                    found=type(metrics).__name__,
                    origin=origin,
                )
            )
    else:
        system_metrics = _parse_system_metrics(source_file, parsed, metrics.get("system"), diagnostics)
        custom_metric_names = _parse_custom_refs(source_file, parsed, metrics.get("custom"), diagnostics)
    return EvalConfig(
        origin,
        source_file,
        agent,
        agent_version,
        dataset,
        system_metrics,
        custom_metric_names,
        _parse_run(source_file, parsed, tree.get("run"), diagnostics),
        _parse_sweep(source_file, parsed, tree.get("sweep"), diagnostics),
        tuple(key for key in ("database", "schema", "enabled") if key in tree),
    )


def _parse_dataset_config(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalDatasetConfig | None:
    origin = _origin(parsed, ("dataset",), source_file)
    if value is None:
        return None
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="dataset",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return None
    mapping = value.get("column_mapping")
    columns = EvalColumnMapping()
    if mapping is not None:
        if not isinstance(mapping, dict):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field="dataset.column_mapping",
                    expected="mapping",
                    found=type(mapping).__name__,
                    origin=origin,
                )
            )
        else:
            columns = EvalColumnMapping(
                optional_string(mapping.get("query_text")) or "input_query",
                optional_string(mapping.get("ground_truth")) or "ground_truth",
            )
    return EvalDatasetConfig(
        _optional_string_field(value, "mint", source_file, parsed, diagnostics, ("dataset",)),
        _optional_string_field(value, "name_template", source_file, parsed, diagnostics, ("dataset",)),
        _optional_string_field(value, "source_table_template", source_file, parsed, diagnostics, ("dataset",)),
        columns,
    )


def _parse_system_metrics(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> tuple[EvalSystemMetric, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D("SST-PRS003", artifact=source_file, field="metrics.system", expected="list", found=type(value).__name__)
        )
        return ()
    metrics: list[EvalSystemMetric] = []
    for index, item in enumerate(value):
        origin = _origin(parsed, ("metrics", "system", index), source_file)
        if not isinstance(item, dict):
            diagnostics.append(
                D(
                    "SST-PRS018",
                    artifact=source_file,
                    field="metrics.system",
                    index=index,
                    expected="mapping",
                    found=type(item).__name__,
                    origin=origin,
                )
            )
            continue
        metrics.append(
            EvalSystemMetric(
                origin,
                _optional_string_field(item, "name", source_file, parsed, diagnostics, ("metrics", "system", index)),
                _optional_string_field(item, "version", source_file, parsed, diagnostics, ("metrics", "system", index)),
                _optional_bool_field(item, "gate", source_file, origin, diagnostics),
                _parse_threshold(
                    source_file, f"metrics.system[{index}].threshold", item.get("threshold"), origin, diagnostics
                ),
                _optional_string_field(
                    item, "judge_model", source_file, parsed, diagnostics, ("metrics", "system", index)
                ),
            )
        )
    return tuple(metrics)


def _parse_custom_refs(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D("SST-PRS003", artifact=source_file, field="metrics.custom", expected="list", found=type(value).__name__)
        )
        return ()
    names: list[str] = []
    for index, item in enumerate(value):
        origin = _origin(parsed, ("metrics", "custom", index), source_file)
        if not isinstance(item, str):
            diagnostics.append(
                D(
                    "SST-PRS018",
                    artifact=source_file,
                    field="metrics.custom",
                    index=index,
                    expected="eval_metric() string",
                    found=type(item).__name__,
                    origin=origin,
                )
            )
            continue
        try:
            call = single_template_call(item, "eval_metric")
        except TemplateSyntaxError as exc:
            diagnostics.append(D("SST-LOD004", file=source_file, line=exc.line, col=exc.col, reason=exc.reason))
            continue
        if call is None or len(call.args) != 1:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    file=source_file,
                    line=origin.line or 1,
                    col=origin.col or 1,
                    reason="expected one eval_metric() reference",
                )
            )
            continue
        names.append(str(call.args[0]))
    return tuple(names)


def _parse_run(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalRunConfig | None:
    origin = _origin(parsed, ("run",), source_file)
    if value is None:
        return None
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="run",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return None
    retention_value = value.get("retention")
    retention = EvalRetention()
    if retention_value is not None:
        if not isinstance(retention_value, dict):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field="run.retention",
                    expected="mapping",
                    found=type(retention_value).__name__,
                    origin=origin,
                )
            )
        else:
            retention = EvalRetention(
                _string_tuple(retention_value.get("audit"), "run.retention.audit", origin, diagnostics),
                _string_tuple(retention_value.get("decision"), "run.retention.decision", origin, diagnostics),
                optional_int(retention_value.get("decision_window_days")),
            )
    accept_statuses = _string_tuple(value.get("accept_statuses"), "run.accept_statuses", origin, diagnostics)
    for status in accept_statuses:
        if status not in EVAL_TERMINAL_STATUSES:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=source_file,
                    field="run.accept_statuses",
                    found=status,
                    expected=", ".join(sorted(EVAL_TERMINAL_STATUSES)),
                    origin=origin,
                )
            )
    tier = _optional_string_field(value, "tier", source_file, parsed, diagnostics, ("run",))
    if tier is not None and tier not in {"blocking", "report"}:
        diagnostics.append(
            D(
                "SST-PRS013",
                artifact=source_file,
                field="run.tier",
                found=tier,
                expected="blocking, report",
                origin=origin,
            )
        )
    return EvalRunConfig(
        _optional_string_field(value, "name_template", source_file, parsed, diagnostics, ("run",)),
        _optional_string_field(value, "label", source_file, parsed, diagnostics, ("run",)),
        _optional_string_field(value, "description", source_file, parsed, diagnostics, ("run",)),
        _optional_string_field(value, "variant", source_file, parsed, diagnostics, ("run",)),
        retention,
        tier,
        _optional_int_field(value, "retry", source_file, origin, diagnostics),
        _optional_int_field(value, "concurrency", source_file, origin, diagnostics),
        _optional_int_field(value, "baseline_runs", source_file, origin, diagnostics),
        accept_statuses,
    )


def _parse_sweep(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalSweepConfig | None:
    origin = _origin(parsed, ("sweep",), source_file)
    if value is None:
        return None
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="sweep",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return None
    return EvalSweepConfig(
        _optional_bool_field(value, "enabled", source_file, origin, diagnostics) or False,
        _string_tuple(value.get("models"), "sweep.models", origin, diagnostics),
        _optional_string_field(value, "target", source_file, parsed, diagnostics, ("sweep",)),
        _string_tuple(value.get("hold_constant"), "sweep.hold_constant", origin, diagnostics),
        _optional_bool_field(value, "cost_reporting", source_file, origin, diagnostics) or False,
    )


def _parse_custom_metric(
    source_file: str,
    parsed: ParsedYaml,
    diagnostics: list[Diagnostic],
) -> CustomEvalMetric | None:
    tree = parsed.tree
    origin = _origin(parsed, (), source_file)
    name = _required_string(tree, "name", source_file, parsed, diagnostics)
    if name is None:
        return None
    report_unknown_keys(tree, _CUSTOM_METRIC_KEYS, diagnostics, artifact=source_file, origin=origin)
    meta = tree.get("meta")
    if meta is not None and not isinstance(meta, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="meta",
                expected="mapping",
                found=type(meta).__name__,
                origin=origin,
            )
        )
        meta = {}
    enabled = _optional_bool_field(tree, "enabled", source_file, origin, diagnostics)
    return CustomEvalMetric(
        origin,
        source_file,
        name,
        _optional_string_field(tree, "description", source_file, parsed, diagnostics),
        _optional_string_field(tree, "model", source_file, parsed, diagnostics),
        _parse_score_ranges(source_file, parsed, tree.get("score_ranges"), diagnostics),
        _optional_string_field(tree, "prompt", source_file, parsed, diagnostics),
        _optional_bool_field(tree, "gate_default", source_file, origin, diagnostics),
        _parse_threshold(source_file, "threshold_default", tree.get("threshold_default"), origin, diagnostics),
        True if enabled is None else enabled,
        MappingProxyType({str(key): item for key, item in (meta or {}).items()}),
    )


def _parse_score_ranges(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalScoreRanges | None:
    origin = _origin(parsed, ("score_ranges",), source_file)
    if value is None:
        diagnostics.append(D("SST-PRS002", artifact=source_file, field="score_ranges", origin=origin))
        return None
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="score_ranges",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return None
    parsed_ranges: dict[str, tuple[int, int]] = {}
    for field in ("min_score", "median_score", "max_score"):
        item = value.get(field)
        if (
            not isinstance(item, list)
            or len(item) != 2
            or any(not isinstance(bound, int) or isinstance(bound, bool) for bound in item)
        ):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"score_ranges.{field}",
                    expected="two integers",
                    found=type(item).__name__,
                    origin=origin,
                )
            )
            continue
        parsed_ranges[field] = (item[0], item[1])
    if len(parsed_ranges) != 3:
        return None
    return EvalScoreRanges(parsed_ranges["min_score"], parsed_ranges["median_score"], parsed_ranges["max_score"])


def _parse_threshold(
    source_file: str,
    field: str,
    value: object,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> ThresholdRange | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return None
    minimum = _number(value.get("min"))
    maximum = _number(value.get("max"))
    for key, parsed in (("min", minimum), ("max", maximum)):
        if key in value and parsed is None:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"{field}.{key}",
                    expected="number",
                    found=type(value[key]).__name__,
                    origin=origin,
                )
            )
    return ThresholdRange(minimum, maximum)


def _required_string(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    parsed: ParsedYaml,
    diagnostics: list[Diagnostic],
) -> str | None:
    result = _optional_string_field(value, field, source_file, parsed, diagnostics)
    if field not in value:
        diagnostics.append(D("SST-PRS002", artifact=source_file, field=field, origin=_origin(parsed, (), source_file)))
    return result


def _optional_string_field(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    parsed: ParsedYaml,
    diagnostics: list[Diagnostic],
    path: tuple[str | int, ...] = (),
) -> str | None:
    if field not in value:
        return None
    result = optional_string(value[field])
    if result is None:
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="non-empty string",
                found=type(value[field]).__name__,
                origin=_origin(parsed, (*path, field), source_file),
            )
        )
    return result


def _optional_bool_field(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> bool | None:
    if field not in value:
        return None
    raw = value[field]
    if isinstance(raw, bool):
        return raw
    diagnostics.append(D("SST-PRS019", artifact=source_file, field=field, found=raw, origin=origin))
    return None


def _optional_int_field(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> int | None:
    if field not in value:
        return None
    result = optional_int(value[field])
    if result is None:
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="integer",
                found=type(value[field]).__name__,
                origin=origin,
            )
        )
    return result


def _string_tuple(
    value: object,
    field: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> tuple[str, ...]:
    return checked_strings(value, diagnostics, field=field, artifact=origin.file, origin=origin)


def _origin(parsed: ParsedYaml, path: tuple[str | int, ...], source_file: str) -> Origin:
    position = parsed.line_index.get(path)
    return Origin(source_file, position.line if position else 1, position.col if position else 1)


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None
