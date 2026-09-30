"""Parse an agent's eval config file, and the `evals:` defaults block of `sst_config.yml`.

The defaults are returned apart and never merged into a config, so the validator can tell a
value the config sets from one it inherits.
"""

from __future__ import annotations

from typing import Mapping

from ....domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ....domain.model.eval import (
    EVAL_TERMINAL_STATUSES,
    EvalColumnMapping,
    EvalConfig,
    EvalDatasetConfig,
    EvalDefaults,
    EvalRetention,
    EvalRunConfig,
    EvalSweepConfig,
    EvalSystemMetric,
)
from ....domain.model.reference import TemplateSyntaxError, single_template_call
from ..documents import ParsedYaml
from ..fields import optional_int, optional_string
from .readers import (
    optional_bool_field,
    optional_int_field,
    optional_string_field,
    origin_at,
    parse_threshold,
    required_string,
    string_tuple,
)


def parse_eval_defaults(value: object) -> tuple[EvalDefaults, DiagnosticBag]:
    """Parse the `evals:` block of `sst_config.yml` into the defaults every eval inherits.

    An absent block reads as the empty defaults. `+metrics` is checked for its type and a
    string `+eval_tier` for its value; any other value of the wrong type reads as unset,
    unreported.

    Returns:
        The defaults, and the diagnostics about the block.

    Diagnostics:
        SST-PRS003: the block is not a mapping, or `+metrics` is not a list of strings.
        SST-PRS013: `+eval_tier` is neither `blocking` nor `report`.
    """
    if value is None:
        return EvalDefaults(), DiagnosticBag()
    if not isinstance(value, dict):
        return EvalDefaults(), DiagnosticBag(
            (D("SST-PRS003", artifact="sst_config.yml", field="evals", expected="mapping", found=type(value).__name__),)
        )
    diagnostics: list[Diagnostic] = []
    origin = Origin("sst_config.yml")
    metrics = string_tuple(value.get("+metrics"), "evals.+metrics", origin, diagnostics)
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


def parse_config(loaded: tuple[str, ParsedYaml], diagnostics: list[Diagnostic]) -> EvalConfig:
    """Parse one eval config file, given with its project-relative path.

    Custom metrics are returned as the names their `eval_metric()` references give; the caller
    resolves them. `database`, `schema` and `enabled` are recorded as the location keys the
    file sets, for the validator to refuse. Fields are read, and reported, in this order:
    `agent`, `agent_version`, `dataset`, `metrics`, `run`, `sweep`.

    Diagnostics:
        SST-PRS002: `agent` or `metrics` is absent.
        SST-PRS003: a field has the wrong type, such as a `run:` block that is not a mapping.
        SST-PRS013: `run.tier` or a `run.accept_statuses` entry is not an accepted value.
        SST-PRS018: an entry of `metrics.system` or `metrics.custom` has the wrong type.
        SST-PRS019: a boolean field, such as a system metric's `gate`, is not a boolean.
        SST-LOD004: a `metrics.custom` entry is not exactly one `eval_metric()` reference.
    """
    source_file, parsed = loaded
    tree = parsed.tree
    origin = origin_at(parsed, (), source_file)
    agent = required_string(tree, "agent", source_file, parsed, diagnostics)
    agent_version = optional_string_field(tree, "agent_version", source_file, parsed, diagnostics)
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
    """Parse the `dataset:` block, or return None when it is absent or not a mapping.

    A `column_mapping` column that is absent, or not a non-empty string, reads as its default
    name without a report.

    Diagnostics:
        SST-PRS003: the block or its `column_mapping` is not a mapping, or `mint` or a template
            is not a non-empty string.
    """
    origin = origin_at(parsed, ("dataset",), source_file)
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
        optional_string_field(value, "mint", source_file, parsed, diagnostics, ("dataset",)),
        optional_string_field(value, "name_template", source_file, parsed, diagnostics, ("dataset",)),
        optional_string_field(value, "source_table_template", source_file, parsed, diagnostics, ("dataset",)),
        columns,
    )


def _parse_system_metrics(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> tuple[EvalSystemMetric, ...]:
    """Parse `metrics.system`, skipping each entry that is not a mapping.

    Diagnostics:
        SST-PRS003: the block is not a list, reported without a position, or an entry's field
            has the wrong type.
        SST-PRS018: an entry is not a mapping.
        SST-PRS019: an entry's `gate` is not a boolean.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D("SST-PRS003", artifact=source_file, field="metrics.system", expected="list", found=type(value).__name__)
        )
        return ()
    metrics: list[EvalSystemMetric] = []
    for index, item in enumerate(value):
        origin = origin_at(parsed, ("metrics", "system", index), source_file)
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
                optional_string_field(item, "name", source_file, parsed, diagnostics, ("metrics", "system", index)),
                optional_string_field(item, "version", source_file, parsed, diagnostics, ("metrics", "system", index)),
                optional_bool_field(item, "gate", source_file, origin, diagnostics),
                parse_threshold(
                    source_file, f"metrics.system[{index}].threshold", item.get("threshold"), origin, diagnostics
                ),
                optional_string_field(
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
    """Parse `metrics.custom` into the names its `eval_metric()` references give, in order.

    An entry that is not a string holding exactly one single-argument `eval_metric()` call is
    reported and skipped.

    Diagnostics:
        SST-PRS003: the block is not a list; reported without a position.
        SST-PRS018: an entry is not a string.
        SST-LOD004: an entry's template does not parse, or is not one `eval_metric()` call with
            one argument.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D("SST-PRS003", artifact=source_file, field="metrics.custom", expected="list", found=type(value).__name__)
        )
        return ()
    names: list[str] = []
    for index, item in enumerate(value):
        origin = origin_at(parsed, ("metrics", "custom", index), source_file)
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
    """Parse the `run:` block, or return None when it is absent or not a mapping.

    Retention, the accepted statuses and the tier are read first, in that order, then the
    remaining fields in the order `EvalRunConfig` takes them; reports follow the same order.

    Diagnostics:
        SST-PRS003: the block or its `retention` is not a mapping, a status or retention list
            is not a list of strings, or a string or integer field has the wrong type.
        SST-PRS013: `tier` or an accepted status is not an accepted value.
    """
    origin = origin_at(parsed, ("run",), source_file)
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
    retention = _parse_retention(source_file, value.get("retention"), origin, diagnostics)
    accept_statuses = _parse_accept_statuses(source_file, value, origin, diagnostics)
    tier = _parse_tier(source_file, parsed, value, origin, diagnostics)
    return EvalRunConfig(
        optional_string_field(value, "name_template", source_file, parsed, diagnostics, ("run",)),
        optional_string_field(value, "label", source_file, parsed, diagnostics, ("run",)),
        optional_string_field(value, "description", source_file, parsed, diagnostics, ("run",)),
        optional_string_field(value, "variant", source_file, parsed, diagnostics, ("run",)),
        retention,
        tier,
        optional_int_field(value, "retry", source_file, origin, diagnostics),
        optional_int_field(value, "concurrency", source_file, origin, diagnostics),
        optional_int_field(value, "baseline_runs", source_file, origin, diagnostics),
        accept_statuses,
    )


def _parse_retention(
    source_file: str,
    retention_value: object,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> EvalRetention:
    """Parse `run.retention`; an absent block, or one that is not a mapping, reads as the default.

    Diagnostics:
        SST-PRS003: the block is not a mapping, or `audit` or `decision` is not a list of strings.
    """
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
                string_tuple(retention_value.get("audit"), "run.retention.audit", origin, diagnostics),
                string_tuple(retention_value.get("decision"), "run.retention.decision", origin, diagnostics),
                optional_int(retention_value.get("decision_window_days")),
            )
    return retention


def _parse_accept_statuses(
    source_file: str,
    value: Mapping[str, object],
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> tuple[str, ...]:
    """Parse `run.accept_statuses`; a status that is not terminal is reported and still kept.

    Diagnostics:
        SST-PRS003: the field is not a list of strings.
        SST-PRS013: a status is not a terminal status.
    """
    accept_statuses = string_tuple(value.get("accept_statuses"), "run.accept_statuses", origin, diagnostics)
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
    return accept_statuses


def _parse_tier(
    source_file: str,
    parsed: ParsedYaml,
    value: Mapping[str, object],
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> str | None:
    """Parse `run.tier`; a tier other than `blocking` or `report` is reported and still kept.

    Diagnostics:
        SST-PRS003: the field is not a non-empty string.
        SST-PRS013: the tier is neither `blocking` nor `report`.
    """
    tier = optional_string_field(value, "tier", source_file, parsed, diagnostics, ("run",))
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
    return tier


def _parse_sweep(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalSweepConfig | None:
    """Parse the `sweep:` block, which SST parses but does not run.

    `enabled` and `cost_reporting` read as false when absent or not a boolean.

    Diagnostics:
        SST-PRS003: the block is not a mapping, `models` or `hold_constant` is not a list of
            strings, or `target` is not a non-empty string.
        SST-PRS019: `enabled` or `cost_reporting` is not a boolean.
    """
    origin = origin_at(parsed, ("sweep",), source_file)
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
        optional_bool_field(value, "enabled", source_file, origin, diagnostics) or False,
        string_tuple(value.get("models"), "sweep.models", origin, diagnostics),
        optional_string_field(value, "target", source_file, parsed, diagnostics, ("sweep",)),
        string_tuple(value.get("hold_constant"), "sweep.hold_constant", origin, diagnostics),
        optional_bool_field(value, "cost_reporting", source_file, origin, diagnostics) or False,
    )
