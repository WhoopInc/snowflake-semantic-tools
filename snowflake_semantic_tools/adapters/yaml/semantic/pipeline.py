"""The semantic load: parse, check, attach and build every view, collecting each diagnostic on the way."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ....domain.model.artifact_key import artifact_key
from ....domain.model.dbt import DbtModel, DbtTarget
from ....domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin, Severity
from ....domain.model.project import ResolvedProject, SemanticViewProject
from ....domain.model.reference import TemplateSyntaxError, scan_template_calls
from ....domain.model.registry import SEMANTIC_REGISTRY
from ....domain.model.semantic_view import Relationship, SemanticView
from ....domain.resolve.members import attach_view_members
from ...errors import ProjectError
from ..documents import RawDocuments, discover_yaml, load_documents
from ..fields import mapping
from ..parse import parse_yaml_bytes, read_yaml_mapping
from .build import _build_view
from .checks.authored_keys import _authored_key_diagnostics, _legacy_reference_diagnostics, _member_name_diagnostics
from .checks.dbt import _dbt_column_diagnostics, _dbt_model_diagnostics, _description_diagnostics
from .checks.expressions import _expression_reference_diagnostics, _filter_diagnostics
from .checks.metrics import _metric_cycles, _metric_diagnostics
from .checks.shape import _filter_parse_diagnostics, _metric_parse_diagnostics, _verified_query_diagnostics
from .collect import parse_semantic_project
from .defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from .relationships import (
    _multipath_diagnostics,
    _relationship_cycle_diagnostics,
    _relationship_diagnostics,
    _relationship_parse_diagnostics,
)
from .target import _folder_route_diagnostics, _semantic_view_defaults, _semantic_view_target, _stray_view_diagnostics


@dataclass(frozen=True, slots=True)
class SemanticInputs:
    """What the semantic load reads from SST's own files: the config and every semantic-model document."""

    config: dict[str, Any]
    semantic_models_dir: str
    documents: RawDocuments


def read_semantic_inputs(project_dir: Path) -> SemanticInputs:
    """Read `sst_config.yml`, then discover and parse every semantic-model document once.

    The caller reads these before it loads the dbt target and models, so a broken config or a
    missing semantic-models directory is reported before dbt is consulted.
    """
    config = read_yaml_mapping(project_dir / "sst_config.yml")
    semantic_models_dir = str((config.get("project") or {}).get("semantic_models_dir") or "semantic_models")
    documents = load_documents(discover_yaml(project_dir, semantic_models_dir), parse_yaml_bytes)
    return SemanticInputs(config, semantic_models_dir, documents)


def load_semantic_views_result(
    project_dir: Path,
    inputs: SemanticInputs,
    *,
    target: DbtTarget,
    models: dict[str, DbtModel],
) -> SemanticViewProject:
    """Load healthy views while collecting view-local failures.

    Args:
        inputs: The project config and semantic-model documents, as `read_semantic_inputs` read them.
        target: The dbt target that `{{ target.database }}` and `{{ target.schema }}` name.
        models: The target's dbt models, by casefolded name.
    """
    config = inputs.config
    semantic_models_dir = inputs.semantic_models_dir
    documents = inputs.documents
    parsed = parse_semantic_project(documents, project_dir, semantic_models_dir, models)
    metrics = tuple(
        member.source for member in parsed.members_by_type.get("metric", ()) if isinstance(member.source, MetricDef)
    )
    cycles = _metric_cycles(metrics)
    poisoned_metrics = {name for cycle in cycles for name in cycle[:-1]}
    healthy_metrics = tuple(metric for metric in metrics if metric.name.casefold() not in poisoned_metrics)
    filters = tuple(
        member.source for member in parsed.members_by_type.get("filter", ()) if isinstance(member.source, FilterDef)
    )
    instructions = {
        member.name.casefold(): member.source
        for member in parsed.members_by_type.get("custom_instruction", ())
        if isinstance(member.source, InstructionDef)
    }
    verified_queries = tuple(
        member.source
        for member in parsed.members_by_type.get("verified_query", ())
        if isinstance(member.source, VerifiedQueryDef)
    )
    relationship_members = parsed.members_by_type.get("relationship", ())
    relationships = tuple(member.source for member in relationship_members if isinstance(member.source, Relationship))
    relationship_records = tuple(
        (member.source, member.origin) for member in relationship_members if isinstance(member.source, Relationship)
    )

    views_dir = project_dir / semantic_models_dir / "semantic_views"
    if not views_dir.is_dir():
        raise ProjectError(f"no semantic_views/ directory under {project_dir / semantic_models_dir}")

    out: list[SemanticView] = []
    diagnostics: list[Diagnostic] = list(parsed.diagnostics)
    view_counts: dict[str, int] = {}
    for parsed_view in parsed.views:
        name = parsed_view.name.casefold()
        view_counts[name] = view_counts.get(name, 0) + 1
    diagnostics.extend(
        D(
            "SST-VAL001",
            type="semantic_view",
            name=name,
            subject=artifact_key("semantic_view", name),
        )
        for name, count in sorted(view_counts.items())
        if count > 1
    )
    duplicate_views = {name for name, count in view_counts.items() if count > 1}
    for view in parsed.views:
        if view.poisoned:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=view.origin,
                    file=view.origin.file,
                    line=view.origin.line or 1,
                    col=view.origin.col or 1,
                    reason="malformed table reference",
                    subject=artifact_key("semantic_view", view.name),
                )
            )
    for member in parsed.members:
        if member.poisoned and member.type_name in (
            "metric",
            "filter",
            "verified_query",
        ):
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=member.origin,
                    file=member.origin.file,
                    line=member.origin.line or 1,
                    col=member.origin.col or 1,
                    reason="malformed tables reference",
                    subject=member.key,
                )
            )
    referenced_models = frozenset(
        table for view in parsed.views if not view.poisoned for table in view.declared_tables if table in models
    )
    diagnostics.extend(_folder_route_diagnostics(config, views_dir))
    diagnostics.extend(_stray_view_diagnostics(documents, views_dir))
    diagnostics.extend(_authored_key_diagnostics(documents))
    diagnostics.extend(_member_name_diagnostics(documents))
    diagnostics.extend(_description_diagnostics(parsed.views, metrics))
    diagnostics.extend(_metric_parse_diagnostics(documents, project_dir, semantic_models_dir))
    diagnostics.extend(_filter_parse_diagnostics(documents, project_dir, semantic_models_dir))
    diagnostics.extend(_relationship_parse_diagnostics(documents, project_dir, semantic_models_dir))
    diagnostics.extend(_dbt_model_diagnostics(models, referenced_models))
    diagnostics.extend(_dbt_column_diagnostics(models, referenced_models))
    legacy_diagnostics = _legacy_reference_diagnostics(documents)
    diagnostics.extend(legacy_diagnostics)
    legacy_files = {
        diagnostic.context.get("file")
        for diagnostic in legacy_diagnostics
        if isinstance(diagnostic.context.get("file"), str)
    }
    verified_query_diagnostics = _verified_query_diagnostics(documents, project_dir, semantic_models_dir)
    diagnostics.extend(verified_query_diagnostics)
    variables: dict[str, object] = mapping(config.get("vars"))
    expression_diagnostics = _expression_reference_diagnostics(
        filters + verified_queries,
        models,
        metric_names=frozenset(metric.name.casefold() for metric in metrics),
        variables=variables,
    )
    diagnostics.extend(
        diagnostic
        for diagnostic in expression_diagnostics
        if diagnostic.code not in ("SST-REF034", "SST-REF035")
        or diagnostic.origin is None
        or diagnostic.origin.file not in legacy_files
    )
    diagnostics.extend(_filter_diagnostics(filters))
    metric_diagnostics = _metric_diagnostics(metrics, models, variables)
    diagnostics.extend(
        diagnostic
        for diagnostic in metric_diagnostics
        if diagnostic.code not in ("SST-REF034", "SST-REF035")
        or diagnostic.origin is None
        or diagnostic.origin.file not in legacy_files
    )
    poisoned_metric_subjects = {
        diagnostic.subject
        for diagnostic in metric_diagnostics
        if diagnostic.subject is not None and diagnostic.severity is Severity.ERROR
    }
    poisoned_expression_subjects = {
        diagnostic.subject.casefold()
        for diagnostic in expression_diagnostics
        if diagnostic.subject is not None and diagnostic.severity is Severity.ERROR
    }
    known_models = set(models)
    invalid_metrics: set[str] = set()
    invalid_member_subjects: set[str] = set()
    for metric in metrics:
        for table_name in metric.tables:
            if table_name not in known_models:
                invalid_metrics.add(metric.name.casefold())
                diagnostics.append(
                    D(
                        "SST-MEM003",
                        member=artifact_key("metric", metric.name),
                        name=table_name,
                        subject=artifact_key("metric", metric.name),
                    )
                )
    attachment_members_to_validate: tuple[FilterDef | VerifiedQueryDef, ...] = filters + verified_queries
    for authored_member in attachment_members_to_validate:
        subject = artifact_key(
            "filter" if isinstance(authored_member, FilterDef) else "verified_query", authored_member.name
        )
        for table_name in authored_member.tables:
            if table_name not in known_models:
                invalid_member_subjects.add(subject.casefold())
                diagnostics.append(D("SST-MEM003", member=subject, name=table_name, subject=subject))
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    for cycle in cycles:
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
    healthy_metrics = tuple(
        metric
        for metric in healthy_metrics
        if metric.name.casefold() not in invalid_metrics
        and artifact_key("metric", metric.name) not in poisoned_metric_subjects
    )
    view_table_sets = [
        (artifact_key("semantic_view", view.name), frozenset(view.declared_tables)) for view in parsed.views
    ]
    view_root_key = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert view_root_key is not None
    relationship_diagnostics = _relationship_diagnostics(
        relationships,
        tuple(view_table_sets),
        {relationship.name.casefold(): origin for relationship, origin in relationship_records},
        models,
    )
    diagnostics.extend(relationship_diagnostics)
    diagnostics.extend(_relationship_cycle_diagnostics(relationships, tuple(view_table_sets)))
    diagnostics.extend(_multipath_diagnostics(relationships, healthy_metrics, tuple(view_table_sets)))
    poisoned_relationships = {
        diagnostic.subject
        for diagnostic in relationship_diagnostics
        if diagnostic.code == "SST-VAL203" and diagnostic.subject is not None
    }
    poisoned_member_keys = {
        subject.casefold() for subject in poisoned_metric_subjects | poisoned_relationships if subject is not None
    }
    poisoned_member_keys.update(artifact_key("metric", name) for name in poisoned_metrics)
    poisoned_member_keys.update(poisoned_expression_subjects)
    poisoned_member_keys.update(invalid_member_subjects)
    poisoned_member_keys.update(member.key for member in parsed.members if member.origin.file in legacy_files)
    known_relationships = {relationship.name.casefold(): relationship for relationship in relationships}
    for metric in healthy_metrics:
        for relationship_name in metric.using_relationships:
            relationship = known_relationships.get(relationship_name.casefold())
            if relationship is None:
                diagnostics.append(
                    D(
                        "SST-VAL214",
                        metric=metric.name,
                        relationship=relationship_name,
                        subject=artifact_key("metric", metric.name),
                        origin=metric.origin,
                    )
                )
                poisoned_member_keys.add(artifact_key("metric", metric.name).casefold())
                continue
            if metric.tables and relationship.from_table.casefold() != metric.tables[0]:
                diagnostics.append(
                    D(
                        "SST-VAL114",
                        metric=metric.name,
                        other=relationship_name,
                        name=metric.tables[0],
                        subject=artifact_key("metric", metric.name),
                        origin=metric.origin,
                    )
                )
                poisoned_member_keys.add(artifact_key("metric", metric.name).casefold())
    attachment_members = tuple(
        (dataclasses.replace(member, poisoned=True) if member.key in poisoned_member_keys else member)
        for member in parsed.members
    )
    view_named_members: dict[str, frozenset[str]] = {}
    known_instruction_names = frozenset(instructions)
    for parsed_view in parsed.views:
        raw_instructions = parsed_view.source.get("custom_instructions")
        instruction_values = raw_instructions if isinstance(raw_instructions, list) else []
        names: set[str] = set()
        for raw in instruction_values:
            try:
                calls = scan_template_calls(str(raw))
            except TemplateSyntaxError as exc:
                diagnostics.append(
                    D(
                        "SST-LOD004",
                        origin=parsed_view.origin,
                        file=parsed_view.origin.file,
                        line=exc.line,
                        col=exc.col,
                        reason=exc.reason,
                        subject=artifact_key("semantic_view", parsed_view.name),
                    )
                )
                continue
            for call in calls:
                view_subject = artifact_key("semantic_view", parsed_view.name)
                if call.function != "custom_instructions":
                    diagnostics.append(
                        D(
                            "SST-REF041",
                            origin=parsed_view.origin,
                            subject=view_subject,
                            artifact=view_subject,
                            function=call.function,
                            field="custom_instructions",
                        )
                    )
                    continue
                if len(call.args) != 1:
                    diagnostics.append(
                        D(
                            "SST-REF042",
                            origin=parsed_view.origin,
                            subject=view_subject,
                            artifact=view_subject,
                            detail=f"custom_instructions() takes one name, found {len(call.args)} in {call.raw}",
                        )
                    )
                    continue
                instruction_name = call.args[0].casefold()
                if instruction_name not in known_instruction_names:
                    diagnostics.append(
                        D(
                            "SST-REF039",
                            origin=parsed_view.origin,
                            subject=view_subject,
                            artifact=view_subject,
                            name=call.args[0],
                        )
                    )
                    continue
                names.add(instruction_name)
        view_named_members[artifact_key("semantic_view", parsed_view.name)] = frozenset(names)
    poisoned_views = {
        diagnostic.subject.casefold()
        for diagnostic in diagnostics
        if diagnostic.subject is not None
        and diagnostic.subject.casefold().startswith(artifact_key("semantic_view", ""))
        and diagnostic.severity is Severity.ERROR
    }
    poisoned_views.update(artifact_key("semantic_view", view.name).casefold() for view in parsed.views if view.poisoned)
    # A view authored with the legacy globals is rejected by SST-REF034; building
    # it would only report the same call again as an internal error.
    poisoned_views.update(
        artifact_key("semantic_view", view.name).casefold() for view in parsed.views if view.origin.file in legacy_files
    )
    metric_dependencies = {
        member.key: tuple(artifact_key("metric", name) for name in member.source.referenced_metrics)
        for member in attachment_members
        if member.type_name == "metric" and isinstance(member.source, MetricDef)
    }
    attachment = attach_view_members(
        {artifact: tables for artifact, tables in view_table_sets},
        attachment_members,
        SEMANTIC_REGISTRY,
        view_named_members=view_named_members,
        metric_dependencies=metric_dependencies,
    )
    for document in documents.under(views_dir, view_root_key):
        path = document.abs_path
        for node in document.tree.get(view_root_key) or []:
            if not isinstance(node, dict) or not node.get("name"):
                continue
            if node.get("enabled") is False:
                continue
            if node.get("enabled") is None and _semantic_view_defaults(config, path, views_dir).get("enabled") is False:
                continue
            if str(node["name"]).casefold() in duplicate_views:
                continue
            if artifact_key("semantic_view", node["name"]).casefold() in poisoned_views:
                continue
            view_target = _semantic_view_target(config, path, views_dir, target)
            try:
                out.append(
                    _build_view(
                        node,
                        path,
                        project_dir,
                        view_target,
                        models,
                        attachment_members,
                        attachment,
                        config,
                    )
                )
            except ProjectError as exc:
                view_origin = Origin(path.resolve().relative_to(project_dir.resolve()).as_posix())
                view_subject = artifact_key("semantic_view", node["name"])
                if exc.diagnostics:
                    diagnostics.extend(
                        replace(item, origin=item.origin or view_origin, subject=item.subject or view_subject)
                        for item in exc.diagnostics
                    )
                else:
                    diagnostics.append(
                        D(
                            "SST-PRS123",
                            origin=view_origin,
                            subject=view_subject,
                            artifact=view_subject,
                            detail=str(exc),
                        )
                    )
    resolved = ResolvedProject(
        views=tuple(out),
        attachment=attachment,
        custom_instruction_names=MappingProxyType(
            {artifact.casefold(): tuple(sorted(names)) for artifact, names in view_named_members.items()}
        ),
        diagnostics=DiagnosticBag(diagnostics),
    )
    return SemanticViewProject(resolved.views, resolved.diagnostics)
