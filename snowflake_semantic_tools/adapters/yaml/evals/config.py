"""Parse an agent's eval config file, and the `evals:` block of `sst_config.yml`.

A config's own settings are its `config:` block, keyed as the `evals:` `+` keys without their
`+`; the catalog loader resolves them over the folder routes of the config's agent and the
block root, and fills in what stands in for an unset value -- `metrics`, `metric_version` and
`judge_model` -- before validation.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.evals.readers import (
    optional_bool_field,
    optional_string_field,
    origin_at,
    parse_threshold,
    removed_agent_key,
    required_string,
    string_tuple,
)
from snowflake_semantic_tools.adapters.yaml.fields import optional_string
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE, resolve_settings
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_KNOWN_TERMINAL_STATUSES,
    EVAL_MINT_POLICIES,
    SYSTEM_EVAL_METRICS,
    EvalColumnMapping,
    EvalConfig,
    EvalDatasetConfig,
    EvalDefaults,
    EvalRunConfig,
    EvalSystemMetric,
    eval_defaults,
)
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

# Settings an earlier draft read outside `config:`, by the path that held them, with where each is
# set now. Each is an error, so a file still writing one learns it no longer does anything.
_MOVED: Mapping[tuple[str, ...], str] = MappingProxyType(
    {
        ("agent_version",): "an eval config sets it under config:, as config.agent_version",
        ("run", "tier"): "an eval config sets it under config:, as config.eval_tier",
        ("run", "retry"): "an eval config sets it under config:, as config.retry",
        ("run", "concurrency"): "an eval config sets it under config:, as config.concurrency",
        ("run", "baseline_runs"): "an eval config sets it under config:, as config.baseline_runs",
        ("run", "retention"): "an eval config sets its retention class under config:, as config.retention",
    }
)


def parse_eval_defaults(value: object, *, file: str = CONFIG_FILE) -> tuple[EvalDefaults, DiagnosticBag]:
    """Parse the `evals:` block of the configuration file into its root's settings.

    `file` is how diagnostics name the configuration file the run read.

    An absent block reads as no settings. The root's and each folder route's `+metrics` are
    checked for their type and each name they list, and a string `+eval_tier` at the root for
    its value; any other value of the wrong type reads as unset, unreported. A `+metrics` name
    that is not a system metric is reported and left out, and a repeated one is kept once.

    Returns:
        The root's settings, and the diagnostics about the block.

    Diagnostics:
        SST-PRS003: the block is not a mapping, or a `+metrics` is not a list of strings.
        SST-PRS013: a `+metrics` name is not a system metric, or `+eval_tier` is neither
            `blocking` nor `report`.
    """
    if value is None:
        return EvalDefaults(), DiagnosticBag()
    if not isinstance(value, dict):
        return EvalDefaults(), DiagnosticBag(
            (D("SST-PRS003", artifact=file, field="evals", expected="mapping", found=type(value).__name__),)
        )
    diagnostics: list[Diagnostic] = []
    origin = Origin(file)
    for path, block in _route_blocks(value, ("evals",)):
        if "+metrics" in block:
            _default_metrics(block.get("+metrics"), ".".join((*path, "+metrics")), file, origin, diagnostics)
    eval_tier = optional_string(value.get("+eval_tier"))
    if eval_tier is not None and eval_tier not in {"blocking", "report"}:
        diagnostics.append(
            D(
                "SST-PRS013",
                artifact=file,
                field="evals.+eval_tier",
                found=eval_tier,
                expected="blocking, report",
                origin=origin,
            )
        )
    return eval_defaults(resolve_settings(value, ())), DiagnosticBag(tuple(diagnostics))


def _route_blocks(
    block: Mapping[str, object], path: tuple[str, ...]
) -> list[tuple[tuple[str, ...], dict[str, object]]]:
    """Return the block and each folder route below it, with its path, depth first."""
    found: list[tuple[tuple[str, ...], dict[str, object]]] = [(path, {str(key): item for key, item in block.items()})]
    for key, child in block.items():
        if not str(key).startswith("+") and isinstance(child, dict):
            found.extend(_route_blocks(child, (*path, str(key))))
    return found


def _default_metrics(
    value: object, field: str, file: str, origin: Origin, diagnostics: list[Diagnostic]
) -> tuple[str, ...]:
    """Read one `+metrics`, or an eval config's `config.metrics`: the system metrics it names, each once.

    Diagnostics:
        SST-PRS003: the value is not a list of strings.
        SST-PRS013: a name is not a system metric; it is left out.
    """
    names: list[str] = []
    for name in string_tuple(value, field, origin, diagnostics):
        if name not in SYSTEM_EVAL_METRICS:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=file,
                    field=field,
                    found=name,
                    expected=", ".join(sorted(SYSTEM_EVAL_METRICS)),
                    origin=origin,
                )
            )
        elif name not in names:
            names.append(name)
    return tuple(names)


def parse_config(loaded: tuple[str, ParsedYaml], diagnostics: list[Diagnostic]) -> EvalConfig:
    """Parse one eval config file, given with its project-relative path.

    Custom metrics are returned as the names their `eval_metric()` references give; the caller
    resolves them. `database`, `schema` and `enabled` are recorded as the location keys the
    file sets, for the validator to refuse, and the `config:` block as written, for the caller
    to resolve. The file names no agent: its folder says which it belongs to. Fields are read,
    and reported, in this order: `agent`, then `name`, `config`, `dataset`, `metrics`, `run`,
    `sweep`; then the settings written outside `config:`.

    Diagnostics:
        SST-PRS126: the file writes `agent:`.
        SST-PRS002: `name` or `metrics` is absent.
        SST-PRS003: a field has the wrong type, such as a `run:` block that is not a mapping.
        SST-PRS013: a `run.accept_statuses` entry, or a `config.metrics` name, is not an
            accepted value.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports the `config:` block.
        SST-PRS126: the file writes `agent_version`, or a `run:` setting, outside `config:`.
        SST-PRS018: an entry of `metrics.system` or `metrics.custom` has the wrong type.
        SST-PRS019: a boolean field, such as a system metric's `gate`, is not a boolean.
        Every code `required_call` lists, for a `metrics.custom` entry that is not one
            `eval_metric()` reference.
        SST-CFG044: the file declares a `sweep:` block, which this release does not run.
    """
    source_file, parsed = loaded
    tree = parsed.tree
    origin = origin_at(parsed, (), source_file)
    diagnostics.extend(removed_agent_key(source_file, parsed))
    name = required_string(tree, "name", source_file, parsed, diagnostics)
    item_config = tree.get("config")
    _check_item_config(source_file, parsed, item_config, diagnostics)
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
    run = _parse_run(source_file, parsed, tree.get("run"), diagnostics)
    _reject_sweep(source_file, parsed, tree.get("sweep"), diagnostics)
    diagnostics.extend(_moved_settings(source_file, parsed))
    return EvalConfig(
        origin,
        source_file,
        dataset,
        system_metrics,
        custom_metric_names,
        run,
        tuple(key for key in ("database", "schema", "enabled") if key in tree),
        name,
        item_config,
    )


def _check_item_config(source_file: str, parsed: ParsedYaml, value: object, diagnostics: list[Diagnostic]) -> None:
    """Check a config's `config:` block against the `evals:` settings, and the system metrics it names.

    Diagnostics:
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports them.
        SST-PRS013: a `config.metrics` name is not a system metric.
    """
    diagnostics.extend(
        item_config_diagnostics(
            "evals",
            value,
            subject=source_file,
            origin_of=lambda key: origin_at(parsed, ("config", key) if key else ("config",), source_file),
        )
    )
    if isinstance(value, Mapping) and isinstance(value.get("metrics"), list):
        origin = origin_at(parsed, ("config", "metrics"), source_file)
        _default_metrics(value["metrics"], "config.metrics", source_file, origin, diagnostics)


def _moved_settings(source_file: str, parsed: ParsedYaml) -> tuple[Diagnostic, ...]:
    """Report each setting the file writes where an earlier draft read it, outside `config:`.

    Diagnostics:
        SST-PRS126: once per such setting, naming the `config:` key that replaced it.
    """
    found: list[Diagnostic] = []
    for path, reason in _MOVED.items():
        holder = parsed.tree.get(path[0]) if len(path) > 1 else parsed.tree
        if isinstance(holder, Mapping) and path[-1] in holder:
            found.append(
                D(
                    "SST-PRS126",
                    origin=origin_at(parsed, path, source_file),
                    artifact=source_file,
                    field=".".join(path),
                    reason=reason,
                )
            )
    return tuple(found)


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
        SST-PRS013: `mint` is neither `auto` nor `never`.
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
    mint = optional_string_field(value, "mint", source_file, parsed, diagnostics, ("dataset",))
    if mint is not None and mint not in EVAL_MINT_POLICIES:
        diagnostics.append(
            D(
                "SST-PRS013",
                artifact=source_file,
                field="dataset.mint",
                found=mint,
                expected=", ".join(sorted(EVAL_MINT_POLICIES)),
                origin=origin,
            )
        )
    return EvalDatasetConfig(
        mint,
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
        Every code `required_call` lists, for an entry that is not one `eval_metric()` call.
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
        located = Origin(source_file, origin.line, origin.col)
        functions = reference_functions("eval_config.metrics.custom")
        found = required_call(item, functions, located, field="metrics.custom", artifact=source_file)
        if isinstance(found, Diagnostic):
            diagnostics.append(found)
            continue
        names.append(str(found.args[0]))
    return tuple(names)


def _parse_run(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalRunConfig | None:
    """Parse the `run:` block, or return None when it is absent or not a mapping.

    The accepted statuses are read first, then the remaining fields in the order
    `EvalRunConfig` takes them; reports follow the same order.

    Diagnostics:
        SST-PRS003: the block is not a mapping, the status list is not a list of strings, or a
            string field has the wrong type.
        SST-PRS013: an accepted status is not an accepted value.
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
    accept_statuses = _parse_accept_statuses(source_file, value, origin, diagnostics)
    return EvalRunConfig(
        optional_string_field(value, "name_template", source_file, parsed, diagnostics, ("run",)),
        optional_string_field(value, "label", source_file, parsed, diagnostics, ("run",)),
        optional_string_field(value, "description", source_file, parsed, diagnostics, ("run",)),
        optional_string_field(value, "variant", source_file, parsed, diagnostics, ("run",)),
        accept_statuses,
    )


def _parse_accept_statuses(
    source_file: str,
    value: Mapping[str, object],
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> tuple[str, ...]:
    """Parse `run.accept_statuses`; a status no run is known to end in is reported and still kept.

    Diagnostics:
        SST-PRS003: the field is not a list of strings.
        SST-PRS013: a status is not a terminal status a run is known to report.
    """
    accept_statuses = string_tuple(value.get("accept_statuses"), "run.accept_statuses", origin, diagnostics)
    for status in accept_statuses:
        if status not in EVAL_KNOWN_TERMINAL_STATUSES:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=source_file,
                    field="run.accept_statuses",
                    found=status,
                    expected=", ".join(sorted(EVAL_KNOWN_TERMINAL_STATUSES)),
                    origin=origin,
                )
            )
    return accept_statuses


def _reject_sweep(source_file: str, parsed: ParsedYaml, value: object, diagnostics: list[Diagnostic]) -> None:
    """Refuse a `sweep:` block: SST 1.0 runs no model sweeps, so accepting one would do nothing.

    Diagnostics:
        SST-CFG044: the config declares a `sweep:` block.
    """
    if value is not None:
        diagnostics.append(D("SST-CFG044", key="sweep", origin=origin_at(parsed, ("sweep",), source_file)))
