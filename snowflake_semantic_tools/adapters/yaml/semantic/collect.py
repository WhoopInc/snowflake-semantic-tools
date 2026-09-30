"""Collect every view and member of a project into one immutable, unresolved parsed project."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from ....domain.model.dbt import DbtModel
from ....domain.model.diagnostic import DiagnosticBag, Origin
from ....domain.model.project import ParsedMember, ParsedProject, ParsedView
from ....domain.model.reference import TemplateCall, TemplateSyntaxError, scan_template_calls
from ....domain.model.registry import SEMANTIC_REGISTRY
from ....domain.model.semantic_view import ColumnKind
from ..documents import RawDocuments
from .nodes import _node_origin, _safe_table_refs
from .readers import load_filters, load_instructions, load_metrics, load_verified_queries
from .relationships import load_relationships


def parse_semantic_project(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
    models: Mapping[str, DbtModel] | None = None,
) -> ParsedProject:
    """Parse loaded trees into immutable unresolved view and member records."""
    views: list[ParsedView] = []
    members: list[ParsedMember] = []
    view_root = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert view_root is not None
    for document in documents.documents:
        for index, node in enumerate(document.tree.get(view_root) or []):
            if not isinstance(node, dict) or not node.get("name"):
                continue
            calls: list[TemplateCall] = []
            malformed = False
            for raw in node.get("tables") or []:
                try:
                    calls.extend(scan_template_calls(str(raw)))
                except TemplateSyntaxError:
                    malformed = True
            views.append(
                ParsedView(
                    name=str(node["name"]),
                    origin=_node_origin(document, view_root, index),
                    source_path=document.path,
                    source=MappingProxyType({str(key): value for key, value in node.items()}),
                    declared_tables=_safe_table_refs(node.get("tables")),
                    template_calls=tuple(calls),
                    poisoned=malformed,
                )
            )

    metrics = load_metrics(documents, project_dir, semantic_models_dir)
    filters = load_filters(documents, project_dir, semantic_models_dir)
    instructions = load_instructions(documents, project_dir, semantic_models_dir)
    verified_queries = load_verified_queries(documents, project_dir, semantic_models_dir)
    relationship_records, relationship_diagnostics = load_relationships(
        documents, project_dir, semantic_models_dir, models
    )
    members.extend(
        ParsedMember(
            "metric",
            metric.name,
            metric.origin or Origin("<unknown>"),
            metric,
            metric.tables if metric.has_tables_key else None,
            metric.template_calls,
            metric.poisoned,
        )
        for metric in metrics
    )
    members.extend(
        ParsedMember(
            "filter",
            filter_def.name,
            filter_def.origin or Origin("<unknown>"),
            filter_def,
            filter_def.tables,
            filter_def.template_calls,
            filter_def.poisoned,
        )
        for filter_def in filters
    )
    members.extend(
        ParsedMember(
            "custom_instruction",
            instruction.name,
            instruction.origin or Origin("<unknown>"),
            instruction,
            None,
            (),
            instruction.poisoned,
        )
        for instruction in instructions.values()
    )
    members.extend(
        ParsedMember(
            "verified_query",
            query.name,
            query.origin or Origin("<unknown>"),
            query,
            query.tables,
            query.template_calls,
            query.poisoned,
        )
        for query in verified_queries
    )
    members.extend(
        ParsedMember(
            "relationship",
            relationship.name,
            origin,
            relationship,
            (relationship.from_table.casefold(), relationship.to_table.casefold()),
        )
        for relationship, origin in relationship_records
    )
    for model in (models or {}).values():
        model_origin = Origin(
            model.patch_path or model.original_file_path or "<dbt-manifest>",
            dbt_node=model.unique_id,
        )
        for column in model.columns:
            if column.column_type not in (
                ColumnKind.FACT.value,
                ColumnKind.DIMENSION.value,
            ):
                continue
            members.append(
                ParsedMember(
                    column.column_type,
                    f"{model.name}.{column.name}",
                    model_origin,
                    (model, column),
                    (model.name.casefold(),),
                )
            )
    return ParsedProject(
        tuple(views), tuple(members), DiagnosticBag((*documents.diagnostics, *relationship_diagnostics))
    )
