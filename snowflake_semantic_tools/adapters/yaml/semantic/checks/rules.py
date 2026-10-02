"""Run the content rules of the semantic load: files, prose, hardcoded names, and each view.

`_rule_diagnostics` is the one call the load makes for them, after the view instructions are
known and before views are poisoned, so an error here keeps its view from being built.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.fields import mapping
from snowflake_semantic_tools.adapters.yaml.semantic.checks.files import _file_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.text import (
    _hardcoded_name_diagnostics,
    _overlap_diagnostics,
    _unresolved_prose_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.views import ViewInputs, _view_rule_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedProject
from snowflake_semantic_tools.domain.model.semantic_view import Relationship


def _setting(config: Mapping[str, Any], key: str) -> int | None:
    """One positive integer under `validation:`; None when unset or not one (config validation reports it)."""
    block = config.get("validation")
    value = block.get(key) if isinstance(block, dict) else None
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _rule_diagnostics(
    documents: RawDocuments,
    parsed: ParsedProject,
    models: Mapping[str, DbtModel],
    config: Mapping[str, Any],
    view_instructions: Mapping[str, frozenset[str]],
    unavailable: Mapping[str, str],
) -> tuple[Diagnostic, ...]:
    """Run the content rules in order: files, hardcoded names, metric descriptions, instructions, views.

    Diagnostics:
        SST-VAL004: a metric's description is shorter than `validation.description_floor`.
        As `_file_diagnostics`, `_hardcoded_name_diagnostics`, `_overlap_diagnostics`,
        `_unresolved_prose_diagnostics` and `_view_rule_diagnostics` document them.
    """
    by_type = parsed.members_by_type

    def sources(type_name: str, kind: type[Any]) -> tuple[Any, ...]:
        return tuple(member.source for member in by_type.get(type_name, ()) if isinstance(member.source, kind))

    metrics: tuple[MetricDef, ...] = sources("metric", MetricDef)
    filters: tuple[FilterDef, ...] = sources("filter", FilterDef)
    queries: tuple[VerifiedQueryDef, ...] = sources("verified_query", VerifiedQueryDef)
    relationships: tuple[Relationship, ...] = sources("relationship", Relationship)
    instructions = {item.name.casefold(): item for item in sources("custom_instruction", InstructionDef)}
    floor = _setting(config, "description_floor")
    inputs = ViewInputs(
        models,
        metrics,
        filters,
        relationships,
        instructions,
        view_instructions,
        unavailable=unavailable,
        description_floor=floor,
        instruction_budget=_setting(config, "instruction_budget"),
    )
    short_metrics = tuple(
        D(
            "SST-VAL004",
            origin=metric.origin,
            subject=artifact_key("metric", metric.name),
            type="metric",
            name=metric.name,
            size=len(metric.description),
            expected=floor,
        )
        for metric in metrics
        if floor is not None and metric.description is not None and len(metric.description) < floor
    )
    return (
        *_file_diagnostics(documents),
        *_hardcoded_name_diagnostics(parsed.views, metrics, filters, queries),
        *short_metrics,
        *_overlap_diagnostics(filters, instructions, mapping(config.get("vars"))),
        *_unresolved_prose_diagnostics(
            instructions,
            frozenset(metric.name.casefold() for metric in metrics),
            frozenset(item.name.casefold() for item in filters),
        ),
        *_view_rule_diagnostics(parsed.views, inputs),
    )
