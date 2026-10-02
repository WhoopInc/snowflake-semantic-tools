"""Collect every view and member of a project into one immutable, unresolved parsed project."""

from __future__ import annotations

from collections.abc import Mapping
from functools import partial
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocument, RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _node_origin, _safe_table_refs
from snowflake_semantic_tools.adapters.yaml.semantic.readers import (
    load_filters,
    load_instructions,
    load_metrics,
    load_verified_queries,
)
from snowflake_semantic_tools.adapters.yaml.semantic.relationships import load_relationships
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedMember, ParsedProject, ParsedView
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind, Relationship
from snowflake_semantic_tools.domain.parse.records import mutable_records, registered_root_keys, root_key_diagnostics
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls


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

    Diagnostics:
        SST-LOD021, SST-PRS001: a document's root keys are not registered, as
            `root_key_diagnostics` reports them, after the documents' own diagnostics.
        SST-PRS900: a parser returned a record that is not immutable, after the root keys.
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
    known = registered_root_keys(SEMANTIC_REGISTRY)
    shape = tuple(
        diagnostic
        for document in documents.documents
        for diagnostic in root_key_diagnostics(document.path, document.root_keys, known, partial(_key_line, document))
    )
    mutable = (
        *mutable_records("semantic_view", views),
        *(diagnostic for type_name in SEMANTIC_REGISTRY.members for diagnostic in _mutable_members(type_name, members)),
    )
    return ParsedProject(
        views, members, DiagnosticBag((*documents.diagnostics, *shape, *mutable, *relationship_diagnostics))
    )


def _key_line(document: RawDocument, key: str) -> int | None:
    position = document.position((key,))
    return position.line if position is not None else None


def _mutable_members(type_name: str, members: tuple[ParsedMember, ...]) -> tuple[Diagnostic, ...]:
    """Report the parsed members of one type whose record a parser left mutable (SST-PRS900)."""
    return mutable_records(type_name, (member.source for member in members if member.type_name == type_name))


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
