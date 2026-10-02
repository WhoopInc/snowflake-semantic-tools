"""Parse the custom LLM-judge metrics under the project's eval metrics directory.

Every `.yml` or `.yaml` file below the directory, the suffix in any case and at any depth, is
one metric, read in sorted path order. A file that does not parse, or has no usable `name`, is
reported and skipped, and the rest still load.
"""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.evals.readers import (
    optional_bool_field,
    optional_string_field,
    origin_at,
    parse_threshold,
    required_string,
)
from snowflake_semantic_tools.adapters.yaml.fields import report_unknown_keys
from snowflake_semantic_tools.adapters.yaml.parse import read_yaml_file
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.eval import CustomEvalMetric, EvalScoreRanges

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


def load_custom_metrics(
    project_dir: Path,
    directory: str,
    diagnostics: list[Diagnostic],
) -> tuple[CustomEvalMetric, ...]:
    """Load every custom metric file under `directory`, in sorted path order.

    Returns:
        The metrics with a usable name; `()` when the directory does not exist.

    Diagnostics:
        SST-LOD018: a metric file cannot be read.
        SST-LOD006: a metric file is not UTF-8.
        SST-LOD004: a template in a metric file is unterminated or nested.
        SST-LOD001: a metric file is not valid YAML.
        SST-LOD005: a metric file writes a key twice in one mapping.
        SST-LOD003: a metric file holds only whitespace or comments.
        SST-LOD008: a metric file holds more than one document.
        SST-LOD002: a metric file's root is not a mapping.
        SST-PRS002: a metric's `name` or `score_ranges` is absent.
        SST-PRS003: a metric's field has the wrong type.
        SST-PRS004: a metric sets a key SST does not read.
        SST-PRS019: a metric's `gate_default` or `enabled` is not a boolean.
    """
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


def _parse_custom_metric(
    source_file: str,
    parsed: ParsedYaml,
    diagnostics: list[Diagnostic],
) -> CustomEvalMetric | None:
    """Parse one custom metric file, or return None when it has no usable `name`.

    A metric without a usable name is not read further, so only its name is reported. An
    absent or non-boolean `enabled` reads as true, and a `meta` that is not a mapping as empty.

    Diagnostics:
        SST-PRS002: `name` or `score_ranges` is absent.
        SST-PRS003: a field has the wrong type.
        SST-PRS004: a key SST does not read.
        SST-PRS019: `gate_default` or `enabled` is not a boolean.
    """
    tree = parsed.tree
    origin = origin_at(parsed, (), source_file)
    name = required_string(tree, "name", source_file, parsed, diagnostics)
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
    enabled = optional_bool_field(tree, "enabled", source_file, origin, diagnostics)
    return CustomEvalMetric(
        origin,
        source_file,
        name,
        optional_string_field(tree, "description", source_file, parsed, diagnostics),
        optional_string_field(tree, "model", source_file, parsed, diagnostics),
        _parse_score_ranges(source_file, parsed, tree.get("score_ranges"), diagnostics),
        optional_string_field(tree, "prompt", source_file, parsed, diagnostics),
        optional_bool_field(tree, "gate_default", source_file, origin, diagnostics),
        parse_threshold(source_file, "threshold_default", tree.get("threshold_default"), origin, diagnostics),
        True if enabled is None else enabled,
        MappingProxyType({str(key): item for key, item in (meta or {}).items()}),
    )


def _parse_score_ranges(
    source_file: str,
    parsed: ParsedYaml,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalScoreRanges | None:
    """Parse `score_ranges`, whose `min_score`, `median_score` and `max_score` are integer pairs.

    Returns:
        The bands, or None when the field is absent, is not a mapping, or has an unusable
        band; every unusable band is reported.

    Diagnostics:
        SST-PRS002: the field is absent.
        SST-PRS003: the field is not a mapping, or a band is not two integers.
    """
    origin = origin_at(parsed, ("score_ranges",), source_file)
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
