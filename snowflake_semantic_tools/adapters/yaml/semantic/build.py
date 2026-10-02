"""Build one resolved `SemanticView` from its node and the members attached to it.

`_build_view` runs the build as phases, each taking what the earlier ones returned. The phases
for the view's own keys -- tables, columns, variables, tags -- live here; the members attached to
the view are built in `build_members`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.fields import mapping
from snowflake_semantic_tools.adapters.yaml.semantic.build_members import (
    _expression,
    _instruction_parts,
    _metric_names,
    _names,
    _require,
    _Resolver,
    _view_filters,
    _view_metrics,
    _view_verified_queries,
    _with_variable_names,
)
from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _as_str_tuple
from snowflake_semantic_tools.domain.diagnostics import D, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel, DbtSource, DbtTarget
from snowflake_semantic_tools.domain.model.project import ParsedMember
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Relationship,
    SemanticView,
    Table,
    Tag,
    Variable,
)
from snowflake_semantic_tools.domain.parse.template import single_template_call
from snowflake_semantic_tools.domain.resolve.template import TAG_NAME, ResolveContext, resolve_scalar
from snowflake_semantic_tools.domain.sql import Sql, boolean, datatype, is_datatype, literal, number
from snowflake_semantic_tools.domain.validate.sql import qualified_name_problem


def _build_view(
    node: dict[str, Any],
    path: Path,
    project_dir: Path,
    target: DbtTarget,
    models: dict[str, DbtModel],
    members: tuple[ParsedMember, ...],
    attachment: Mapping[str, tuple[str, ...]],
    config: dict[str, Any],
    sources: tuple[DbtSource, ...] = (),
) -> SemanticView:
    """Build one view from its node and the members attached to it, one phase at a time.

    The phases run in this order, and each raises `ProjectError` at the first problem it finds,
    so the order decides which problem a broken view reports: select the attached members, set
    up the context, then tables, columns, metric names, metrics and their windows, filters,
    instructions, verified queries, variables, tags, and assemble. A phase reads only what the
    earlier phases returned.

    Raises:
        ProjectError: A table, column, member, variable or tag does not resolve. Its diagnostics
            name the problem; one without diagnostics is reported by its message.
    """
    name = str(node["name"])
    selected = _select_members(artifact_key("semantic_view", name), members, attachment)
    view = _View(
        name,
        path,
        Origin(path.resolve().relative_to(project_dir.resolve()).as_posix()),
        models,
        DbtCatalog("v12", None, None, tuple(models.values()), sources=sources),
        mapping(config.get("vars")),
    )
    tables, logical_by_model = _view_tables(node, view)
    columns = _view_columns(models, logical_by_model)
    resolver = _member_resolver(view, logical_by_model, selected)
    metrics = _view_metrics(selected.metrics, selected.relationships, resolver)
    entity_filters, standalone_filters = _view_filters(selected.filters, resolver)
    instructions = tuple(selected.instructions.values())
    sql_parts, question_parts = _instruction_parts(instructions, standalone_filters, logical_by_model)
    queries = _view_verified_queries(
        selected.verified_queries, logical_by_model, resolver.metric_names, config, view.catalog
    )
    variables = _view_variables(node, view)
    # Rewrites the resolved expressions, so it runs once the metrics and filters are built.
    metrics, entity_filters = _with_variable_names(variables, metrics, entity_filters)
    tags = _view_tags(node, view, config, target)
    max_staleness = node.get("max_staleness")
    source_path, source_files = _source_files(path, project_dir, selected.attached, models, logical_by_model)
    fqn = target.fqn(name)
    _require(qualified_name_problem(fqn, artifact=view.key, subject=view.key))
    return SemanticView(
        fqn=fqn,
        tables=tables,
        relationships=selected.relationships,
        variables=variables,
        columns=tuple(columns + entity_filters),
        metrics=tuple(metrics),
        comment=(node.get("description") or "").strip() or None,
        ai_sql_generation="\n\n".join(sql_parts) or None,
        ai_question_categorization="\n\n".join(question_parts) or None,
        custom_instruction_names=tuple(item.name for item in instructions),
        verified_queries=queries,
        max_staleness=f"{max_staleness} seconds" if max_staleness is not None else None,
        tags=tags,
        source_path=source_path,
        source_files=source_files,
        referenced_models=tuple(sorted(logical_by_model)),
    )


@dataclass(frozen=True, slots=True)
class _Members:
    """The parsed members attached to one view, by type, each type in attachment order.

    Attributes:
        attached: Every attached member, whatever its type: their files are sources of the view.
        instructions: The custom instructions, by casefolded name.
    """

    attached: tuple[ParsedMember, ...]
    metrics: tuple[MetricDef, ...]
    filters: tuple[FilterDef, ...]
    instructions: Mapping[str, InstructionDef]
    verified_queries: tuple[VerifiedQueryDef, ...]
    relationships: tuple[Relationship, ...]


def _select_members(
    view_key: str, members: tuple[ParsedMember, ...], attachment: Mapping[str, tuple[str, ...]]
) -> _Members:
    """Select the parsed members attached to the view, and split them by type."""
    attached = tuple(member for member in members if view_key in attachment.get(member.key, ()))
    return _Members(
        attached=attached,
        metrics=_sources(attached, "metric", MetricDef),
        filters=_sources(attached, "filter", FilterDef),
        instructions={
            member.name.casefold(): member.source
            for member in attached
            if member.type_name == "custom_instruction" and isinstance(member.source, InstructionDef)
        },
        verified_queries=_sources(attached, "verified_query", VerifiedQueryDef),
        relationships=_sources(attached, "relationship", Relationship),
    )


_Source = TypeVar("_Source")


def _sources(attached: tuple[ParsedMember, ...], type_name: str, kind: type[_Source]) -> tuple[_Source, ...]:
    """Return the records of the attached members of one type, in attachment order."""
    return tuple(
        member.source for member in attached if member.type_name == type_name and isinstance(member.source, kind)
    )


@dataclass(frozen=True, slots=True)
class _View:
    """The view being built, and the project inputs every phase of its build reads.

    Attributes:
        origin: The view's file, relative to the project: where its own keys' diagnostics point.
        models: The target's dbt models, by casefolded name.
        catalog: The same models, as the expression resolver reads them.
        variables: The project's `vars:`.
    """

    name: str
    path: Path
    origin: Origin
    models: Mapping[str, DbtModel]
    catalog: DbtCatalog
    variables: Mapping[str, object]

    @property
    def key(self) -> str:
        """The view's artifact key, which the view's build diagnostics name."""
        return artifact_key("semantic_view", self.name)


def _member_resolver(view: _View, logical_by_model: dict[str, str], selected: _Members) -> _Resolver:
    """Name the view's metrics, and set up how its members' expressions resolve with those names."""
    return _Resolver(
        view_key=view.key,
        path=view.path,
        catalog=view.catalog,
        logical_by_model=logical_by_model,
        metric_names=_metric_names(selected.metrics, logical_by_model),
        instruction_names=frozenset(selected.instructions),
        variables=view.variables,
    )


def _view_tables(node: Mapping[str, Any], view: _View) -> tuple[tuple[Table, ...], dict[str, str]]:
    """Build the view's tables, and each table's logical name by lower-cased model name.

    Diagnostics:
        SST-PRS109: when a `table_config` key names a model no table entry names.

    Raises:
        ProjectError: A table entry is not a one-argument `ref()`, names no model or one an
            earlier entry names, or has a malformed `table_config` entry; or `table_config`
            names a table that is not in `tables`.
    """
    # Tables keep DECLARATION order -- that is authored information and the golden
    # preserves it. Members are sorted later, by the renderer.
    tables: list[Table] = []
    logical_by_model: dict[str, str] = {}
    table_config = node.get("table_config") or {}
    for raw in node.get("tables") or []:
        model_key, table = _view_table(raw, view, table_config, logical_by_model)
        logical_by_model[model_key] = table.logical_name
        tables.append(table)
    stray = (
        [str(key) for key in table_config if str(key).lower() not in logical_by_model]
        if isinstance(table_config, dict)
        else []
    )
    if stray:
        diagnostic = D("SST-PRS109", artifact=view.key, name=stray[0], subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return tuple(tables), logical_by_model


def _view_table(
    raw: object, view: _View, table_config: object, logical_by_model: Mapping[str, str]
) -> tuple[str, Table]:
    """Build one table entry against the dbt models and the entries declared before it.

    Diagnostics:
        SST-DBT011, as `_source_table` reports it, when the entry is one `{{ source() }}` call.
        SST-REF044: when the entry is not one `{{ ref('<model>') }}` call.
        SST-REF001: when the entry names no dbt model.
        SST-PRS006: when the entry names a model an earlier entry names.

    Raises:
        ProjectError: For each diagnostic above, and when the table's `table_config` entry has
            malformed synonyms or `distinct_range`.
    """
    source = single_template_call(str(raw), "source")
    if source is not None and len(source.args) == 2:
        return _source_table(source.args, view, table_config, logical_by_model)
    call = single_template_call(str(raw), "ref")
    if call is None or len(call.args) != 1:
        diagnostic = D("SST-REF044", artifact=view.key, found=repr(raw))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    model_name = call.args[0]
    model = view.models.get(model_name.lower())
    if model is None:
        diagnostic = D("SST-REF001", model=model_name, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    logical = model_name.upper()
    if logical in logical_by_model.values():
        # Role-playing tables are not supported in 1.0, so one physical table
        # cannot appear twice under two logical names.
        diagnostic = D("SST-PRS006", type="table", name=model_name)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    per_table = table_config.get(model_name) if isinstance(table_config, dict) else None
    table_synonyms = _as_str_tuple(per_table.get("synonyms")) if isinstance(per_table, dict) else ()
    distinct_range = _distinct_range(per_table, path=view.path, view_name=view.name, table_name=model_name)
    (logical,) = _names((logical,), view.key, None)
    if qualified_name_problem(model.relation_name, artifact=view.key, subject=view.key) is not None:
        diagnostic = D("SST-REF019", value=model.relation_name, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return model_name.lower(), Table(
        logical_name=logical,
        fqn=model.relation_name,
        primary_key=_names(model.primary_key, view.key, None),
        unique_keys=tuple(_names(key, view.key, None) for key in model.unique_keys),
        synonyms=table_synonyms,
        distinct_range=distinct_range,
    )


def _source_table(
    args: tuple[str, ...], view: _View, table_config: object, logical_by_model: Mapping[str, str]
) -> tuple[str, Table]:
    """Build a table from one `{{ source('<source>', '<table>') }}` entry: a relation without columns.

    Its key is `<source>.<table>`, casefolded, and its logical name the table's name.

    Diagnostics:
        SST-DBT011: when no dbt source declares the pair, or it has no relation.
        SST-PRS006: when an earlier entry names a table of the same logical name.

    Raises:
        ProjectError: For each diagnostic above.
    """
    source_name, table_name = args
    pair = f"{source_name}.{table_name}"
    found = next(
        (
            item
            for item in view.catalog.sources
            if item.relation_name and f"{item.source_name}.{item.name}".casefold() == pair.casefold()
        ),
        None,
    )
    if found is None or found.relation_name is None:
        diagnostic = D("SST-DBT011", value=pair, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    (logical,) = _names((table_name.upper(),), view.key, None)
    if logical in logical_by_model.values():
        diagnostic = D("SST-PRS006", type="table", name=table_name)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    per_table = table_config.get(table_name) if isinstance(table_config, dict) else None
    synonyms = _as_str_tuple(per_table.get("synonyms")) if isinstance(per_table, dict) else ()
    relation = found.relation_name.upper()
    _require(qualified_name_problem(relation, artifact=view.key, subject=view.key))
    return pair.casefold(), Table(
        logical_name=logical, fqn=relation, primary_key=(), unique_keys=(), synonyms=synonyms, distinct_range=None
    )


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
        view_key = artifact_key("semantic_view", view_name)
        detail = f"table_config.{table_name}.distinct_range needs a start and an end column"
        diagnostic = D("SST-PRS028", artifact=view_key, detail=detail, subject=view_key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    start, end = _names((str(value["start"]), str(value["end"])), artifact_key("semantic_view", view_name), None)
    return start, end


def _view_columns(models: Mapping[str, DbtModel], logical_by_model: Mapping[str, str]) -> list[Column]:
    """Build the facts and dimensions of each table in turn, leaving out unset and excluded columns.

    Raises:
        ProjectError: A column's role is not a known column kind (SST-DBT003).
    """
    columns: list[Column] = []
    for model_key, logical in logical_by_model.items():
        model = models.get(model_key)
        if model is None:
            # A dbt source: SST reads no columns of one.
            continue
        columns.extend(
            _view_column(model, column, logical)
            for column in model.columns
            if column.column_type is not None and not column.excluded
        )
    return columns


def _view_column(model: DbtModel, column: DbtColumn, logical: str) -> Column:
    """Build one fact or dimension from its dbt column; a time dimension renders as a dimension.

    Diagnostics:
        SST-DBT003: when the column's role is not a known column kind.

    Raises:
        ProjectError: The column's role is not a known column kind.
    """
    try:
        kind = ColumnKind(column.column_type)
    except ValueError as exc:
        role = D(
            "SST-DBT003",
            subject=f"dbt_model:{model.name}",
            model=f"{model.name}.{column.name}",
            found=column.column_type,
        )
        raise ProjectError(role.message, diagnostics=(role,)) from exc
    if kind is ColumnKind.TIME_DIMENSION:
        kind = ColumnKind.DIMENSION
    subject = f"dbt_model:{model.name}"
    (name,) = _names((column.name,), subject, None)
    return Column(
        table=logical,
        name=name,
        kind=kind,
        expr=_expression(f"{logical}.{name}", kind=kind.value, name=name, subject=subject, origin=None),
        comment=column.description,
        synonyms=column.synonyms,
        sample_values=column.sample_values,
        is_enum=column.is_enum is True,
    )


def _view_variables(node: Mapping[str, Any], view: _View) -> tuple[Variable, ...]:
    """Build the view's variables in declaration order.

    Raises:
        ProjectError: A variable is malformed, or its default does not fit its type.
    """
    return tuple(_variable(value, path=view.path, view_name=view.name) for value in node.get("variables") or [])


def _sql_value(value: object) -> Sql:
    """The SQL literal for a YAML scalar: a boolean or number as written, anything else a string.

    Raises:
        ValueError: a number is not finite, or a string holds a NUL.
    """
    if isinstance(value, bool):
        return boolean(value)
    if isinstance(value, (int, float)):
        return number(value)
    return literal(str(value))


def _variable(value: object, *, path: Path, view_name: str) -> Variable:
    """Build one view variable, its default rendered as the SQL literal of its type.

    Raises:
        ProjectError: The variable lacks a name, a type or a default, or a BOOLEAN, NUMBER or
            string variable's default is not of that type.
    """
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
    subject = artifact_key("semantic_view", view_name)
    if not is_datatype(data_type):
        diagnostic = D(
            "SST-PRS003",
            artifact=subject,
            field=f"variables.{value['name']}.data_type",
            expected="a Snowflake data type",
            found=repr(value["data_type"]),
            subject=subject,
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    try:
        default = _sql_value(raw_default)
    except ValueError as exc:
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} has an invalid default: {exc}") from exc
    (name,) = _names((str(value["name"]),), subject, None)
    return Variable(
        name=name,
        data_type=datatype(data_type),
        default=default,
        comment=str(value.get("description") or "").strip() or None,
    )


def _view_tags(node: Mapping[str, Any], view: _View, config: dict[str, Any], target: DbtTarget) -> tuple[Tag, ...]:
    """Build the view's tags in declaration order.

    Diagnostics:
        SST-PRS027: when `tags:` is not a list.

    Raises:
        ProjectError: `tags:` is not a list, or a tag does not build (see `_tag`).
    """
    raw_tags = node.get("tags")
    if raw_tags is not None and not isinstance(raw_tags, list):
        diagnostic = D("SST-PRS027", artifact=view.key, found=type(raw_tags).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    tag_names = _tag_names(config, target)
    return tuple(_tag(value, view=view, tag_names=tag_names) for value in raw_tags or [])


def _tag_names(config: dict[str, Any], target: DbtTarget) -> dict[str, str]:
    """Name each tag `tags:` in `sst_config.yml` declares: its `default_prefix` and its upper-cased name."""
    block = config.get("tags") or {}
    if not isinstance(block, dict):
        return {}
    prefix = (
        str(block.get("default_prefix") or "")
        .replace("{{ target.database }}", target.database)
        .replace("{{ target.schema }}", target.schema)
    )
    return {str(name): f"{prefix}.{str(name).upper()}" for name in block if name != "default_prefix"}


def _tag(value: object, *, view: _View, tag_names: Mapping[str, str]) -> Tag:
    """Build one tag: its name one `tag()` call naming a tag under `tags:` in `sst_config.yml`.

    Diagnostics:
        SST-PRS027: when the entry lacks a name or a value, or its name holds no template call.
        SST-REF003, SST-REF004, SST-REF041, SST-REF015: when its name is not one one-name `tag()` call.
        SST-REF028: when the call names no declared tag.
        SST-REF019: when the tag's name is not a three-part name.
        SST-PRS026: when the value is longer than 256 characters.

    Raises:
        ProjectError: For each diagnostic above.
    """
    if not isinstance(value, dict) or not value.get("name") or "value" not in value:
        diagnostic = D("SST-PRS027", artifact=view.key, found=repr(value))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    if "{{" not in str(value["name"]):
        found = f"the name {value['name']!r}, which is not one tag() call"
        diagnostic = D("SST-PRS027", artifact=view.key, found=found, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    resolved, diagnostics = resolve_scalar(
        str(value["name"]),
        TAG_NAME,
        view.origin,
        ResolveContext(view.catalog, tags=tag_names),
        field="tags.name",
        subject=view.key,
    )
    if diagnostics:
        raise ProjectError("; ".join(item.message for item in diagnostics), diagnostics=tuple(diagnostics))
    if qualified_name_problem(resolved.text, artifact=view.key, subject=view.key) is not None:
        diagnostic = D("SST-REF019", value=resolved.text, origin=view.origin, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    if len(str(value["value"])) > 256:
        call = single_template_call(str(value["name"]), "tag")
        field = call.args[0] if call is not None and call.args else str(value["name"])
        size = len(str(value["value"]))
        diagnostic = D("SST-PRS026", artifact=view.key, field=field, size=size, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return Tag(name=resolved.text, value=str(value["value"]))


def _source_files(
    path: Path,
    project_dir: Path,
    attached: tuple[ParsedMember, ...],
    models: Mapping[str, DbtModel],
    logical_by_model: Mapping[str, str],
) -> tuple[str, tuple[str, ...]]:
    """Return the view file's project-relative path, and every file the view is built from, sorted.

    Those are the view's own file, each attached member's, and each of its models' dbt files.
    """
    source_path = path.resolve().relative_to(project_dir.resolve()).as_posix()
    source_files = {source_path}
    source_files.update(member.origin.file for member in attached)
    for model_key in logical_by_model:
        model = models.get(model_key)
        model_path = (model.patch_path or model.original_file_path) if model is not None else None
        if model_path:
            source_files.add(model_path)
    return source_path, tuple(sorted(source_files))
