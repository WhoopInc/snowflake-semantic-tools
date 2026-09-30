"""Collect every view and member of a project into one immutable, unresolved parsed project."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ....domain.model.dbt import DbtModel
from ....domain.model.diagnostic import DiagnosticBag, Origin
from ....domain.model.project import ParsedMember, ParsedProject, ParsedView
from ....domain.model.reference import TemplateCall, TemplateSyntaxError, scan_template_calls
from ....domain.model.registry import SEMANTIC_REGISTRY
from ....domain.model.semantic_view import ColumnKind, Relationship
from ..documents import RawDocument, RawDocuments
from .defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from .nodes import _node_origin, _safe_table_refs
from .readers import load_filters, load_instructions, load_metrics, load_verified_queries
from .relationships import load_relationships


def parse_semantic_project(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
    models: Mapping[str, DbtModel] | None = None,
) -> ParsedProject:
    """Parse loaded trees into immutable unresolved view and member records.

    The views come first, in document order. The members follow by type -- metrics,
    filters, custom instructions, verified queries, relationships -- and then the fact and
    dimension columns of the dbt models. The readers run in that order too, so when more
    than one cannot read its members, the first one's error is the one raised.
    """
    views = _parse_views(documents)
    metrics = load_metrics(documents, project_dir, semantic_models_dir)
    filters = load_filters(documents, project_dir, semantic_models_dir)
    instructions = load_instructions(documents, project_dir, semantic_models_dir)
    verified_queries = load_verified_queries(documents, project_dir, semantic_models_dir)
    relationship_records, relationship_diagnostics = load_relationships(
        documents, project_dir, semantic_models_dir, models
    )
    members = (
        *_authored_members(metrics, filters, instructions, verified_queries),
        *_relationship_members(relationship_records),
        *_column_members(models),
    )
    return ParsedProject(views, members, DiagnosticBag((*documents.diagnostics, *relationship_diagnostics)))


def _parse_views(documents: RawDocuments) -> tuple[ParsedView, ...]:
    """Record every named view node of every document, in document order."""
    view_root = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert view_root is not None
    views: list[ParsedView] = []
    for document in documents.documents:
        for index, node in enumerate(document.tree.get(view_root) or []):
            if not isinstance(node, dict) or not node.get("name"):
                continue
            views.append(_parsed_view(document, view_root, index, node))
    return tuple(views)


def _parsed_view(document: RawDocument, view_root: str, index: int, node: dict[str, Any]) -> ParsedView:
    """Record one view node, poisoned when one of its `tables` entries is a malformed template."""
    calls: list[TemplateCall] = []
    malformed = False
    for raw in node.get("tables") or []:
        try:
            calls.extend(scan_template_calls(str(raw)))
        except TemplateSyntaxError:
            malformed = True
    return ParsedView(
        name=str(node["name"]),
        origin=_node_origin(document, view_root, index),
        source_path=document.path,
        source=MappingProxyType({str(key): value for key, value in node.items()}),
        declared_tables=_safe_table_refs(node.get("tables")),
        template_calls=tuple(calls),
        poisoned=malformed,
    )


def _authored_members(
    metrics: tuple[MetricDef, ...],
    filters: tuple[FilterDef, ...],
    instructions: Mapping[str, InstructionDef],
    verified_queries: tuple[VerifiedQueryDef, ...],
) -> tuple[ParsedMember, ...]:
    """Record the metrics, filters, custom instructions and verified queries, in that order.

    A metric without a `tables:` key declares no tables (None): it attaches by the models
    its expression refs. A custom instruction declares none either: views attach it by name.
    """
    return (
        *(
            _authored("metric", metric, metric.tables if metric.has_tables_key else None, metric.template_calls)
            for metric in metrics
        ),
        *(_authored("filter", filter_def, filter_def.tables, filter_def.template_calls) for filter_def in filters),
        *(_authored("custom_instruction", instruction, None, ()) for instruction in instructions.values()),
        *(_authored("verified_query", query, query.tables, query.template_calls) for query in verified_queries),
    )


def _authored(
    type_name: str,
    record: MetricDef | FilterDef | InstructionDef | VerifiedQueryDef,
    declared_tables: tuple[str, ...] | None,
    template_calls: tuple[TemplateCall, ...],
) -> ParsedMember:
    return ParsedMember(
        type_name,
        record.name,
        record.origin or Origin("<unknown>"),
        record,
        declared_tables,
        template_calls,
        record.poisoned,
    )


def _relationship_members(records: tuple[tuple[Relationship, Origin], ...]) -> tuple[ParsedMember, ...]:
    """Record each relationship, declaring its two tables, casefolded."""
    return tuple(
        ParsedMember(
            "relationship",
            relationship.name,
            origin,
            relationship,
            (relationship.from_table.casefold(), relationship.to_table.casefold()),
        )
        for relationship, origin in records
    )


def _column_members(models: Mapping[str, DbtModel] | None) -> tuple[ParsedMember, ...]:
    """Record each fact and dimension column of the dbt models, declaring its model's table."""
    members: list[ParsedMember] = []
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
    return tuple(members)
