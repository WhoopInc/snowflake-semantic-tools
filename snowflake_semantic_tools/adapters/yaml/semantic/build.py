"""Build one resolved `SemanticView` from its node and the members attached to it."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NoReturn

from ....domain.model.artifact_key import artifact_key
from ....domain.model.compiler import FILTER_EXPR, METRIC_EXPR, VQR_SQL, ResolveContext, resolve_scalar
from ....domain.model.dbt import DbtCatalog, DbtModel, DbtTarget
from ....domain.model.diagnostic import D, Origin
from ....domain.model.project import ParsedMember
from ....domain.model.reference import scan_template_calls, single_template_call
from ....domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    SortKey,
    Table,
    Tag,
    Variable,
    VerifiedQuery,
    Window,
)
from ....domain.model.sql import string_literal
from ...errors import ProjectError
from ..fields import mapping
from .defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from .nodes import _as_str_tuple


def _resolve_expression(
    text: str,
    *,
    policy: Any,
    origin: Origin,
    catalog: DbtCatalog,
    logical_by_model: Mapping[str, str],
    metric_names: Mapping[str, str],
    instruction_names: frozenset[str],
    variables: Mapping[str, object],
    field: str,
) -> str:
    context = ResolveContext(
        catalog,
        metric_names=frozenset(metric_names),
        metric_values=metric_names,
        instruction_names=instruction_names,
        variables=variables,
    )
    resolved, diagnostics = resolve_scalar(
        text,
        policy,
        origin,
        context,
        field=field,
        ref_value=lambda call: logical_by_model.get(call.args[0].casefold(), call.raw),
    )
    if diagnostics:
        raise ProjectError(
            "; ".join(diagnostic.message for diagnostic in diagnostics),
            diagnostics=tuple(diagnostics),
        )
    return resolved.text


def _resolve_verified_query_sql(
    query: VerifiedQueryDef,
    logical_by_model: Mapping[str, str],
    resolved_metric_names: Mapping[str, str],
    config: dict[str, Any],
    catalog: DbtCatalog,
) -> str:
    variables: dict[str, object] = mapping(config.get("vars"))
    return _resolve_expression(
        query.sql,
        policy=VQR_SQL,
        origin=query.origin or Origin("<verified-query>"),
        catalog=catalog,
        logical_by_model=logical_by_model,
        metric_names=resolved_metric_names,
        instruction_names=frozenset(),
        variables=variables,
        field="verified_query.sql",
    )


def _build_view(
    node: dict[str, Any],
    path: Path,
    project_dir: Path,
    target: DbtTarget,
    models: dict[str, DbtModel],
    members: tuple[ParsedMember, ...],
    attachment: Mapping[str, tuple[str, ...]],
    config: dict[str, Any],
) -> SemanticView:
    name = str(node["name"])
    view_key = artifact_key("semantic_view", name)
    attached_members = tuple(member for member in members if view_key in attachment.get(member.key, ()))
    metrics = tuple(
        member.source
        for member in attached_members
        if member.type_name == "metric" and isinstance(member.source, MetricDef)
    )
    filters = tuple(
        member.source
        for member in attached_members
        if member.type_name == "filter" and isinstance(member.source, FilterDef)
    )
    instructions = {
        member.name.casefold(): member.source
        for member in attached_members
        if member.type_name == "custom_instruction" and isinstance(member.source, InstructionDef)
    }
    verified_queries = tuple(
        member.source
        for member in attached_members
        if member.type_name == "verified_query" and isinstance(member.source, VerifiedQueryDef)
    )
    relationships = tuple(
        member.source
        for member in attached_members
        if member.type_name == "relationship" and isinstance(member.source, Relationship)
    )
    catalog = DbtCatalog("v12", None, None, tuple(models.values()))
    project_variables: dict[str, object] = mapping(config.get("vars"))

    # Tables keep DECLARATION order -- that is authored information and the golden
    # preserves it. Members are sorted later, by the renderer.
    tables: list[Table] = []
    logical_by_model: dict[str, str] = {}
    table_config = node.get("table_config") or {}
    for raw in node.get("tables") or []:
        call = single_template_call(str(raw), "ref")
        if call is None or len(call.args) != 1:
            diagnostic = D("SST-REF044", artifact=artifact_key("semantic_view", name), found=repr(raw))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        model_name = call.args[0]
        model = models.get(model_name.lower())
        if model is None:
            diagnostic = D("SST-REF001", model=model_name, subject=artifact_key("semantic_view", name))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        logical = model_name.upper()
        if logical in logical_by_model.values():
            # Role-playing tables are not supported in 1.0, so one physical table
            # cannot appear twice under two logical names.
            diagnostic = D("SST-PRS006", type="table", name=model_name)
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        logical_by_model[model_name.lower()] = logical
        per_table = table_config.get(model_name) if isinstance(table_config, dict) else None
        table_synonyms = _as_str_tuple(per_table.get("synonyms")) if isinstance(per_table, dict) else ()
        distinct_range = _distinct_range(per_table, path=path, view_name=name, table_name=model_name)
        tables.append(
            Table(
                logical_name=logical,
                fqn=model.relation_name,
                primary_key=tuple(c.upper() for c in model.primary_key),
                unique_keys=tuple(tuple(column.upper() for column in key) for key in model.unique_keys),
                synonyms=table_synonyms,
                distinct_range=distinct_range,
            )
        )

    columns: list[Column] = []
    for model_key, logical in logical_by_model.items():
        for col in models[model_key].columns:
            if col.column_type is None or col.excluded:
                continue
            try:
                kind = ColumnKind(col.column_type)
            except ValueError as exc:
                role = D(
                    "SST-DBT003",
                    subject=f"dbt_model:{models[model_key].name}",
                    model=f"{models[model_key].name}.{col.name}",
                    found=col.column_type,
                )
                raise ProjectError(role.message, diagnostics=(role,)) from exc
            if kind is ColumnKind.TIME_DIMENSION:
                kind = ColumnKind.DIMENSION
            columns.append(
                Column(
                    table=logical,
                    name=col.name.upper(),
                    kind=kind,
                    expr=f"{logical}.{col.name.upper()}",
                    comment=col.description,
                    synonyms=col.synonyms,
                    sample_values=col.sample_values,
                    is_enum=col.is_enum,
                )
            )

    attached: list[Metric] = []
    resolved_metric_names: dict[str, str] = {}
    for metric in metrics:
        metric_name = metric.name.casefold()
        referenced = metric.tables or metric.referenced_models
        owner = logical_by_model[referenced[0]] if len(referenced) == 1 else None
        resolved_metric_names[metric_name] = metric.name.upper() if owner is None else f"{owner}.{metric.name.upper()}"

    for metric in metrics:
        referenced = metric.tables or metric.referenced_models
        owner = logical_by_model[referenced[0]] if len(referenced) == 1 else None
        outside = next(
            (entry for entry in metric.non_additive if entry.table and entry.table.casefold() not in logical_by_model),
            None,
        )
        if outside is not None:
            diagnostic = D(
                "SST-VAL118",
                metric=metric.name,
                value=f"{outside.table}.{outside.dimension}",
                subject=artifact_key("semantic_view", name),
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        expr = _resolve_expression(
            metric.expr,
            policy=METRIC_EXPR,
            origin=metric.origin or Origin(str(path)),
            catalog=catalog,
            logical_by_model=logical_by_model,
            metric_names=resolved_metric_names,
            instruction_names=frozenset(instructions),
            variables=project_variables,
            field="metric.expression",
        )
        window: Window | None = None
        if metric.window is not None and owner is not None:
            _require_reachable(metric, owner, relationships, logical_by_model, artifact_key("semantic_view", name))

            def resolve(text: str, metric: MetricDef = metric) -> str:
                return _resolve_expression(
                    text,
                    policy=METRIC_EXPR,
                    origin=metric.origin or Origin(str(path)),
                    catalog=catalog,
                    logical_by_model=logical_by_model,
                    metric_names=resolved_metric_names,
                    instruction_names=frozenset(instructions),
                    variables=project_variables,
                    field="metric.window",
                )

            window = Window(
                partition_by=tuple(resolve(text) for text in metric.window.partition_by),
                partition_excluding=tuple(resolve(text) for text in metric.window.partition_excluding),
                order_by=tuple(
                    SortKey(resolve(entry.ref), entry.descending, entry.nulls_first) for entry in metric.window.order_by
                ),
                frame=metric.window.frame,
            )
        attached.append(
            Metric(
                name=metric.name.upper(),
                expr=expr,
                table=owner,
                comment=metric.description,
                synonyms=metric.synonyms,
                using_relationships=metric.using_relationships,
                non_additive_by=tuple(entry.key for entry in metric.non_additive),
                access_modifier=metric.access_modifier,
                window=window,
            )
        )

    entity_filters: list[Column] = []
    standalone_filters: list[FilterDef] = []
    for filter_def in filters:
        if not filter_def.entity_level:
            standalone_filters.append(filter_def)
            continue
        referenced = filter_def.tables or tuple(
            call.args[0] for call in scan_template_calls(filter_def.expr) if call.function == "ref" and call.args
        )
        if len(referenced) != 1:
            raise ProjectError(f"filter {filter_def.name!r} must resolve to exactly one table")
        model_name = referenced[0].casefold()
        expr = _resolve_expression(
            filter_def.expr,
            policy=FILTER_EXPR,
            origin=filter_def.origin or Origin(str(path)),
            catalog=catalog,
            logical_by_model=logical_by_model,
            metric_names=resolved_metric_names,
            instruction_names=frozenset(instructions),
            variables=project_variables,
            field="filter.expression",
        )
        entity_filters.append(
            Column(
                table=logical_by_model[model_name],
                name=filter_def.name.upper(),
                kind=ColumnKind.FILTER,
                expr=expr,
                comment=filter_def.description,
            )
        )

    attached_relationships = relationships

    view_instructions = list(instructions.values())
    sql_instruction_parts = [item.ai_sql_generation for item in view_instructions if item.ai_sql_generation]
    sql_instruction_parts.extend(_standalone_filter_instruction(item, logical_by_model) for item in standalone_filters)
    question_parts = [item.ai_question_categorization for item in view_instructions if item.ai_question_categorization]

    attached_queries = tuple(
        VerifiedQuery(
            name=query.name.upper(),
            question=query.question,
            sql=_resolve_verified_query_sql(query, logical_by_model, resolved_metric_names, config, catalog),
            verified_at=query.verified_at,
            verified_by=query.verified_by,
            onboarding_question=query.onboarding_question,
        )
        for query in verified_queries
    )

    variables = tuple(_variable(value, path=path, view_name=name) for value in node.get("variables") or [])
    for variable in variables:
        attached = [_replace_metric_variable_name(metric, variable.name) for metric in attached]
        entity_filters = [
            _replace_column_variable_name(entity_filter, variable.name) for entity_filter in entity_filters
        ]
    raw_tags = node.get("tags")
    if raw_tags is not None and not isinstance(raw_tags, list):
        diagnostic = D("SST-PRS027", artifact=artifact_key("semantic_view", name), found=type(raw_tags).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    tags = tuple(_tag(value, config=config, target=target, path=path, view_name=name) for value in raw_tags or [])
    max_staleness = node.get("max_staleness")

    source_path = path.resolve().relative_to(project_dir.resolve()).as_posix()
    source_files = {source_path}
    source_files.update(member.origin.file for member in attached_members)
    for model_key in logical_by_model:
        model = models[model_key]
        model_path = model.patch_path or model.original_file_path
        if model_path:
            source_files.add(model_path)
    return SemanticView(
        fqn=target.fqn(name),
        tables=tuple(tables),
        relationships=attached_relationships,
        variables=variables,
        columns=tuple(columns + entity_filters),
        metrics=tuple(attached),
        comment=(node.get("description") or "").strip() or None,
        ai_sql_generation="\n\n".join(sql_instruction_parts) or None,
        ai_question_categorization="\n\n".join(question_parts) or None,
        custom_instruction_names=tuple(item.name for item in view_instructions),
        verified_queries=attached_queries,
        max_staleness=f"{max_staleness} seconds" if max_staleness is not None else None,
        tags=tags,
        source_path=source_path,
        source_files=tuple(sorted(source_files)),
        referenced_models=tuple(sorted(logical_by_model)),
    )


def _require_reachable(
    metric: MetricDef,
    owner: str,
    relationships: tuple[Relationship, ...],
    logical_by_model: Mapping[str, str],
    subject: str,
) -> None:
    """A window's dimensions must be ones the metric's table reaches through the view's relationships."""
    reached = {owner}
    frontier = [owner]
    while frontier:
        table = frontier.pop()
        for relationship in relationships:
            if relationship.from_table == table and relationship.to_table not in reached:
                reached.add(relationship.to_table)
                frontier.append(relationship.to_table)
    assert metric.window is not None
    for field, text in metric.window.references():
        call = single_template_call(text, "ref")
        if call is None or len(call.args) != 2:
            continue
        if logical_by_model.get(call.args[0].casefold()) not in reached:
            diagnostic = D(
                "SST-VAL125",
                metric=metric.name,
                field=field,
                value=text,
                expected=f"a dimension {owner} reaches in this view",
                subject=subject,
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _distinct_range(
    per_table: object,
    *,
    path: Path,
    view_name: str,
    table_name: str,
) -> tuple[str, str] | None:
    if not isinstance(per_table, dict) or "distinct_range" not in per_table:
        return None
    value = per_table["distinct_range"]
    if not isinstance(value, dict) or not value.get("start") or not value.get("end"):
        raise ProjectError(f"{path}: view {view_name} table {table_name} has an invalid distinct_range")
    return str(value["start"]).upper(), str(value["end"]).upper()


def _sql_value(value: object) -> str:
    """The SQL literal for a YAML scalar: a boolean or number as written, anything else a string."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return string_literal(str(value))


def _variable(value: object, *, path: Path, view_name: str) -> Variable:
    if (
        not isinstance(value, dict)
        or not value.get("name")
        or not value.get("data_type")
        or "default_value" not in value
    ):
        raise ProjectError(f"{path}: view {view_name} has an invalid variable {value!r}")
    data_type = str(value["data_type"]).upper()
    raw_default = value["default_value"]
    if data_type == "BOOLEAN" and not isinstance(raw_default, bool):
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} requires a boolean default")
    if data_type.startswith("NUMBER") and (not isinstance(raw_default, (int, float)) or isinstance(raw_default, bool)):
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} requires a numeric default")
    if data_type.startswith(("VARCHAR", "TEXT", "STRING")) and not isinstance(raw_default, str):
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} requires a string default")
    default = _sql_value(raw_default)
    return Variable(
        name=str(value["name"]).upper(),
        data_type=data_type,
        default=default,
        comment=str(value.get("description") or "").strip() or None,
    )


def _tag(value: object, *, config: dict[str, Any], target: DbtTarget, path: Path, view_name: str) -> Tag:
    def invalid(detail: str) -> NoReturn:
        diagnostic = D("SST-REF040", artifact=artifact_key("semantic_view", view_name), detail=detail)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))

    if not isinstance(value, dict) or not value.get("name") or "value" not in value:
        invalid(f"tag {value!r} needs a name and a value")
    call = single_template_call(str(value["name"]), "tag")
    if call is None or len(call.args) != 1:
        invalid(f"tag name must be one tag() call, found {value['name']!r}")
    tags = config.get("tags") or {}
    tag_name = call.args[0]
    if not isinstance(tags, dict) or tag_name not in tags:
        invalid(f"tag('{tag_name}') names no tag under tags: in sst_config.yml")
    prefix = (
        str(tags.get("default_prefix") or "")
        .replace("{{ target.database }}", target.database)
        .replace("{{ target.schema }}", target.schema)
    )
    return Tag(name=f"{prefix}.{tag_name.upper()}", value=str(value["value"]))


def _standalone_filter_instruction(filter_def: FilterDef, logical_by_model: dict[str, str]) -> str:
    if len(filter_def.tables) != 1:
        raise ProjectError(f"standalone filter {filter_def.name!r} must attach to exactly one table")
    table = logical_by_model[filter_def.tables[0]]
    description = filter_def.description or ""
    return _wrap_text(f"For {table}, {filter_def.name} is {filter_def.expr}. {description}".strip())


def _replace_metric_variable_name(metric: Metric, variable_name: str) -> Metric:
    from dataclasses import replace

    expr = re.sub(
        rf"\b{re.escape(variable_name)}\b",
        variable_name.upper(),
        metric.expr,
        flags=re.IGNORECASE,
    )
    return replace(metric, expr=expr)


def _replace_column_variable_name(column: Column, variable_name: str) -> Column:
    from dataclasses import replace

    expr = re.sub(
        rf"\b{re.escape(variable_name)}\b",
        variable_name.upper(),
        column.expr,
        flags=re.IGNORECASE,
    )
    return replace(column, expr=expr)


def _wrap_text(text: str, width: int = 77) -> str:
    import textwrap

    return "\n".join(textwrap.wrap(text, width=width, break_long_words=False, break_on_hyphens=False))
