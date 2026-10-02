"""The check phases of the semantic load, which `pipeline` runs in a fixed order.

Each phase returns its diagnostics instead of appending them anywhere, so the pipeline
alone fixes the order they are reported in. With them a phase returns what the later
phases read: the members by type, the repeated view names and the legacy files, the
findings the member poison reads, and the member keys a relationship check poisons.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.fields import mapping
from snowflake_semantic_tools.adapters.yaml.semantic.checks.authored_keys import (
    _authored_key_diagnostics,
    _legacy_reference_diagnostics,
    _member_name_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import (
    _dbt_column_diagnostics,
    _dbt_model_diagnostics,
    _description_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.deprecated import _deprecated_key_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.expressions import (
    _expression_reference_diagnostics,
    _filter_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.fidelity import _renderer_fidelity_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.instructions import _instruction_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.metrics import _metric_cycles, _metric_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.shape import (
    _filter_parse_diagnostics,
    _metric_parse_diagnostics,
    _verified_query_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.verified_queries import _relative_date_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.adapters.yaml.semantic.relationships import (
    _multipath_diagnostics,
    _relationship_cycle_diagnostics,
    _relationship_diagnostics,
    _relationship_parse_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.target import _folder_route_diagnostics, _stray_view_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtTarget
from snowflake_semantic_tools.domain.model.project import ParsedProject, ParsedView
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.resolve.calls import literal_problem
from snowflake_semantic_tools.domain.resolve.depth import MAX_METRIC_DEPTH, over_deep
from snowflake_semantic_tools.domain.validate.dbt_seam import (
    collapse_diagnostics,
    consumed_model_diagnostics,
    unreadable_model,
)


@dataclass(frozen=True, slots=True)
class LoadContext:
    """The fixed inputs of one semantic load, which every phase reads and none changes.

    Attributes:
        views_dir: `<semantic_models_dir>/semantic_views`; only the views under it are built.
        models: The target's dbt models, by casefolded name.
        catalog: The manifest those models come from, with the models it could not read.
    """

    project_dir: Path
    config: dict[str, Any]
    semantic_models_dir: str
    documents: RawDocuments
    views_dir: Path
    target: DbtTarget
    models: dict[str, DbtModel]
    catalog: DbtCatalog


@dataclass(frozen=True, slots=True)
class SemanticMembers:
    """The parsed members the checks read, by type, and the cycles among the metrics.

    Attributes:
        cycles: The cycles of `metric()` references `_metric_cycles` lists, each once, as
            casefolded metric names of which the last repeats the first; every metric on a
            cycle is in at least one.
        instruction_names: Casefolded names of every custom instruction.
        instructions: Every custom instruction, in parse order.
        relationship_records: Each relationship with the origin it was read from.
    """

    metrics: tuple[MetricDef, ...]
    cycles: tuple[tuple[str, ...], ...]
    filters: tuple[FilterDef, ...]
    instruction_names: frozenset[str]
    verified_queries: tuple[VerifiedQueryDef, ...]
    relationships: tuple[Relationship, ...]
    relationship_records: tuple[tuple[Relationship, Origin], ...]
    instructions: tuple[InstructionDef, ...] = ()


def _typed_members(parsed: ParsedProject) -> SemanticMembers:
    """Split the parsed members by type and find the cycles among the metrics.

    Each type keeps the parse order, which is the order the checks report in.
    """
    by_type = parsed.members_by_type
    metrics = tuple(member.source for member in by_type.get("metric", ()) if isinstance(member.source, MetricDef))
    relationship_members = by_type.get("relationship", ())
    instructions = tuple(
        member.source for member in by_type.get("custom_instruction", ()) if isinstance(member.source, InstructionDef)
    )
    return SemanticMembers(
        metrics=metrics,
        cycles=_metric_cycles(metrics),
        filters=tuple(member.source for member in by_type.get("filter", ()) if isinstance(member.source, FilterDef)),
        instruction_names=frozenset(instruction.name.casefold() for instruction in instructions),
        verified_queries=tuple(
            member.source for member in by_type.get("verified_query", ()) if isinstance(member.source, VerifiedQueryDef)
        ),
        relationships=tuple(
            member.source for member in relationship_members if isinstance(member.source, Relationship)
        ),
        relationship_records=tuple(
            (member.source, member.origin) for member in relationship_members if isinstance(member.source, Relationship)
        ),
        instructions=instructions,
    )


@dataclass(frozen=True, slots=True)
class StructuralChecks:
    """What the structural checks report, and two facts the later phases read.

    Attributes:
        duplicate_views: Casefolded names that more than one view declares.
        legacy_files: The files that use the legacy `table()` and `column()` globals.
    """

    diagnostics: tuple[Diagnostic, ...]
    duplicate_views: frozenset[str]
    legacy_files: frozenset[str]


def _structural_checks(context: LoadContext, parsed: ParsedProject, metrics: tuple[MetricDef, ...]) -> StructuralChecks:
    """Check what the project declares, before any reference in it is resolved.

    Reports, in order: repeated view names, malformed table references, the folder routes,
    view lists outside semantic_views/, authored keys and member names, descriptions, the
    shape of metrics, filters and relationships, the dbt models and columns the views use,
    the legacy globals, and the verified queries.
    """
    declared, duplicate_views = _declaration_diagnostics(parsed)
    documents = _document_diagnostics(context, parsed, metrics)
    legacy = _legacy_reference_diagnostics(context.documents)
    queries = _verified_query_diagnostics(context.documents, context.project_dir, context.semantic_models_dir)
    files = (diagnostic.context.get("file") for diagnostic in legacy)
    return StructuralChecks(
        diagnostics=(*declared, *documents, *legacy, *queries),
        duplicate_views=duplicate_views,
        legacy_files=frozenset(file for file in files if isinstance(file, str)),
    )


def _declaration_diagnostics(parsed: ParsedProject) -> tuple[tuple[Diagnostic, ...], frozenset[str]]:
    """Report the view names declared more than once and every malformed table reference.

    Returns:
        The diagnostics, and the casefolded names that more than one view declares.

    Diagnostics:
        SST-VAL001: more than one view declares a name, compared casefolded.
        SST-LOD004: the tables of a view, metric, filter or verified query are malformed.
    """
    view_counts: dict[str, int] = {}
    for view in parsed.views:
        name = view.name.casefold()
        view_counts[name] = view_counts.get(name, 0) + 1
    diagnostics = [
        D("SST-VAL001", type="semantic_view", name=name, subject=artifact_key("semantic_view", name))
        for name, count in sorted(view_counts.items())
        if count > 1
    ]
    diagnostics.extend(
        _malformed_tables(view.origin, "malformed table reference", artifact_key("semantic_view", view.name))
        for view in parsed.views
        if view.poisoned
    )
    diagnostics.extend(
        _malformed_tables(member.origin, "malformed tables reference", member.key)
        for member in parsed.members
        if member.poisoned and member.type_name in ("metric", "filter", "verified_query")
    )
    return tuple(diagnostics), frozenset(name for name, count in view_counts.items() if count > 1)


def _malformed_tables(origin: Origin, reason: str, subject: str) -> Diagnostic:
    return D(
        "SST-LOD004",
        origin=origin,
        file=origin.file,
        line=origin.line or 1,
        col=origin.col or 1,
        reason=reason,
        subject=subject,
    )


def _document_diagnostics(
    context: LoadContext, parsed: ParsedProject, metrics: tuple[MetricDef, ...]
) -> tuple[Diagnostic, ...]:
    """Check the documents, the folder routes, and the dbt models and columns the views use."""
    documents, project_dir, models_dir = context.documents, context.project_dir, context.semantic_models_dir
    referenced_models = frozenset(
        table for view in parsed.views if not view.poisoned for table in view.declared_tables if table in context.models
    )
    return (
        *consumed_model_diagnostics(context.catalog, referenced_models),
        *collapse_diagnostics(context.catalog, referenced_models),
        *_folder_route_diagnostics(context.config, context.views_dir),
        *_stray_view_diagnostics(documents, context.views_dir),
        *_authored_key_diagnostics(documents),
        *_renderer_fidelity_diagnostics(documents),
        *_deprecated_key_diagnostics(documents),
        *_member_name_diagnostics(documents),
        *_description_diagnostics(parsed.views, metrics),
        *_description_template_diagnostics(parsed),
        *_metric_parse_diagnostics(documents, project_dir, models_dir),
        *_filter_parse_diagnostics(documents, project_dir, models_dir),
        *_relationship_parse_diagnostics(documents, project_dir, models_dir),
        *_dbt_model_diagnostics(context.models, referenced_models),
        *_dbt_column_diagnostics(context.models, referenced_models),
    )


def _description_template_diagnostics(parsed: ParsedProject) -> tuple[Diagnostic, ...]:
    """Report each view, metric and filter description that holds a template expression.

    A description is published as written, so a call in it would reach Snowflake unresolved.

    Diagnostics:
        SST-REF008: when a description holds `{{`.
    """
    described: list[tuple[str, str, Origin | None]] = [
        (artifact_key("semantic_view", view.name), str(view.source.get("description") or ""), view.origin)
        for view in parsed.views
    ]
    described.extend(
        (member.key, member.source.description or "", member.origin)
        for member in parsed.members
        if isinstance(member.source, (MetricDef, FilterDef))
    )
    found = (
        literal_problem(text, origin, field="description", artifact=subject, subject=subject)
        for subject, text, origin in described
    )
    return tuple(diagnostic for diagnostic in found if diagnostic is not None)


@dataclass(frozen=True, slots=True)
class SemanticChecks:
    """What the semantic checks report, and the findings the member poison reads.

    Attributes:
        diagnostics: What the load reports, in order. An SST-REF034/SST-REF035 finding in
            a legacy file is left out: the legacy check already reported that file.
        expression_findings: Every finding of the filter and verified query expression
            checks, none left out.
        metric_findings: Every finding of the metric checks, none left out.
        unknown_table_metrics: Casefolded names of the metrics naming a table that is not a
            dbt model.
        unknown_table_members: Casefolded keys of the filters and verified queries naming one.
    """

    diagnostics: tuple[Diagnostic, ...]
    expression_findings: tuple[Diagnostic, ...]
    metric_findings: tuple[Diagnostic, ...]
    unknown_table_metrics: frozenset[str]
    unknown_table_members: frozenset[str]


def _semantic_checks(context: LoadContext, members: SemanticMembers, legacy_files: frozenset[str]) -> SemanticChecks:
    """Check the references of filters, verified queries and metrics, then their tables and cycles.

    Reports, in order: expression references, filters, custom instructions, verified-query
    dates, metrics, tables that are not dbt models, and metric cycles.
    """
    variables: dict[str, object] = mapping(context.config.get("vars"))
    authored: tuple[FilterDef | VerifiedQueryDef, ...] = members.filters + members.verified_queries
    expression_findings = _expression_reference_diagnostics(
        authored,
        context.models,
        metric_names=frozenset(metric.name.casefold() for metric in members.metrics),
        variables=variables,
    )
    filter_diagnostics = _filter_diagnostics(members.filters, context.models, variables)
    metric_findings = _metric_diagnostics(members.metrics, context.models, variables)
    metric_tables, unknown_table_metrics = _unknown_metric_tables(members.metrics, context.models, context.catalog)
    member_tables, unknown_table_members = _unknown_member_tables(authored, context.models, context.catalog)
    return SemanticChecks(
        diagnostics=(
            *_outside_legacy_files(expression_findings, legacy_files),
            *filter_diagnostics,
            *_instruction_diagnostics(members.instructions),
            *_relative_date_diagnostics(members.verified_queries),
            *_outside_legacy_files(metric_findings, legacy_files),
            *metric_tables,
            *member_tables,
            *_cycle_diagnostics(members),
        ),
        expression_findings=expression_findings,
        metric_findings=metric_findings,
        unknown_table_metrics=unknown_table_metrics,
        unknown_table_members=unknown_table_members,
    )


def _outside_legacy_files(findings: tuple[Diagnostic, ...], legacy_files: frozenset[str]) -> tuple[Diagnostic, ...]:
    """Drop the SST-REF034/SST-REF035 findings in the files the legacy check already reported."""
    return tuple(
        diagnostic
        for diagnostic in findings
        if diagnostic.code not in ("SST-REF034", "SST-REF035")
        or diagnostic.origin is None
        or diagnostic.origin.file not in legacy_files
    )


def _unknown_table(catalog: DbtCatalog, subject: str, table_name: str) -> Diagnostic:
    """Report a declared table that is not a readable dbt model.

    Diagnostics:
        SST-DBT009: the table names a disabled or relationless model.
        SST-MEM003: the table names no dbt model at all.
    """
    return unreadable_model(catalog, table_name, subject=subject) or D(
        "SST-MEM003", member=subject, name=table_name, subject=subject
    )


def _unknown_metric_tables(
    metrics: tuple[MetricDef, ...], models: Mapping[str, DbtModel], catalog: DbtCatalog
) -> tuple[tuple[Diagnostic, ...], frozenset[str]]:
    """Report each metric table that is not a readable dbt model, with the metric names, casefolded."""
    diagnostics: list[Diagnostic] = []
    names: set[str] = set()
    for metric in metrics:
        for table_name in metric.tables:
            if table_name not in models:
                names.add(metric.name.casefold())
                diagnostics.append(_unknown_table(catalog, artifact_key("metric", metric.name), table_name))
    return tuple(diagnostics), frozenset(names)


def _unknown_member_tables(
    members: tuple[FilterDef | VerifiedQueryDef, ...], models: Mapping[str, DbtModel], catalog: DbtCatalog
) -> tuple[tuple[Diagnostic, ...], frozenset[str]]:
    """Report each filter and verified query table that is not a readable model, with the keys casefolded."""
    diagnostics: list[Diagnostic] = []
    keys: set[str] = set()
    for member in members:
        subject = artifact_key("filter" if isinstance(member, FilterDef) else "verified_query", member.name)
        for table_name in member.tables:
            if table_name not in models:
                keys.add(subject.casefold())
                diagnostics.append(_unknown_table(catalog, subject, table_name))
    return tuple(diagnostics), frozenset(keys)


def _cycle_diagnostics(members: SemanticMembers) -> tuple[Diagnostic, ...]:
    """Report each metric cycle once, relating every metric in it, then each over-deep metric.

    Diagnostics:
        SST-REF005: the `metric()` references form a cycle.
        SST-REF900: a metric's references nest more than `MAX_METRIC_DEPTH` deep.
    """
    metric_by_name = {metric.name.casefold(): metric for metric in members.metrics}
    diagnostics: list[Diagnostic] = []
    for cycle in members.cycles:
        participants = tuple(metric_by_name[name] for name in cycle[:-1] if name in metric_by_name)
        origins = tuple(metric.origin for metric in participants if metric.origin is not None)
        diagnostics.append(
            D(
                "SST-REF005",
                cycle=" -> ".join(cycle),
                subject=artifact_key("metric", cycle[0]),
                origin=origins[0] if origins else None,
                related=origins,
            )
        )
    graph = {name: metric.referenced_metrics for name, metric in metric_by_name.items()}
    for name, depth in over_deep(graph):
        diagnostics.append(
            D(
                "SST-REF900",
                found=depth,
                expected=MAX_METRIC_DEPTH,
                subject=artifact_key("metric", name),
                origin=metric_by_name[name].origin,
            )
        )
    return tuple(diagnostics)


def _view_tables(views: tuple[ParsedView, ...]) -> tuple[tuple[str, frozenset[str]], ...]:
    """Pair each view's key with the casefolded tables it declares; malformed tables declare none."""
    return tuple((artifact_key("semantic_view", view.name), frozenset(view.declared_tables)) for view in views)


def _relationship_checks(
    context: LoadContext,
    members: SemanticMembers,
    healthy_metrics: tuple[MetricDef, ...],
    view_tables: tuple[tuple[str, frozenset[str]], ...],
) -> tuple[tuple[Diagnostic, ...], frozenset[str]]:
    """Check relationships against the views that hold their tables, for cycles and for ambiguous paths.

    Returns:
        The diagnostics, and the casefolded keys of the relationships whose tables share no
        view (SST-VAL203): those attach nowhere.
    """
    relationships = members.relationships
    placement = _relationship_diagnostics(
        relationships,
        view_tables,
        {relationship.name.casefold(): origin for relationship, origin in members.relationship_records},
        context.models,
    )
    diagnostics = (
        *placement,
        *_relationship_cycle_diagnostics(relationships, view_tables),
        *_multipath_diagnostics(relationships, healthy_metrics, view_tables),
    )
    unattached = frozenset(
        diagnostic.subject.casefold()
        for diagnostic in placement
        if diagnostic.code in ("SST-VAL205", "SST-VAL207") and diagnostic.subject is not None
    )
    return diagnostics, unattached


def _using_checks(
    members: SemanticMembers, healthy_metrics: tuple[MetricDef, ...]
) -> tuple[tuple[Diagnostic, ...], frozenset[str]]:
    """Check each healthy metric's `using_relationships` against the relationships declared.

    Returns:
        The diagnostics, and the casefolded keys of the metrics they poison.
    """
    known = {relationship.name.casefold(): relationship for relationship in members.relationships}
    diagnostics: list[Diagnostic] = []
    poisoned: set[str] = set()
    for metric in healthy_metrics:
        for relationship_name in metric.using_relationships:
            problem = _using_problem(metric, relationship_name, known.get(relationship_name.casefold()))
            if problem is not None:
                diagnostics.append(problem)
                poisoned.add(artifact_key("metric", metric.name).casefold())
    return tuple(diagnostics), frozenset(poisoned)


def _using_problem(metric: MetricDef, name: str, relationship: Relationship | None) -> Diagnostic | None:
    """Report one `using_relationships` entry of a metric, or return None when it fits.

    Diagnostics:
        SST-REF029: the entry is a `relationship()` call naming no declared relationship.
        SST-VAL214: the entry is a bare name naming no declared relationship.
        SST-VAL114: the relationship does not start from the metric's table.
    """
    subject = artifact_key("metric", metric.name)
    if relationship is None and name in metric.relationship_refs:
        return D("SST-REF029", name=name.casefold(), subject=subject, origin=metric.origin)
    if relationship is None:
        return D("SST-VAL214", metric=metric.name, relationship=name, subject=subject, origin=metric.origin)
    if metric.tables and relationship.from_table.casefold() != metric.tables[0]:
        return D(
            "SST-VAL114",
            metric=metric.name,
            other=name,
            name=metric.tables[0],
            subject=subject,
            origin=metric.origin,
        )
    return None
