"""Build one resolved `SemanticView` from its node and the members attached to it.

`_build_view` runs the build as phases, each taking what the earlier ones returned. The phases
for the view's own keys -- tables, columns, variables, tags -- live here; the members attached to
the view are built in `build_members`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
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
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import view_key
from snowflake_semantic_tools.domain.model.authored import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.config_schema import Setting, setting_value
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel, DbtSource, DbtTarget
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.project import ParsedMember
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Relationship,
    SemanticView,
    Table,
    Tag,
    Variable,
    ViewScope,
)
from snowflake_semantic_tools.domain.parse.template import TemplateCall, single_template_call
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.tags import DeclaredTags, declared_tags, tag_reference
from snowflake_semantic_tools.domain.sql import DefaultMismatch, column_ref, datatype, is_datatype, typed_default
from snowflake_semantic_tools.domain.validate.semantic.scope import view_scope
from snowflake_semantic_tools.domain.validate.semantic_view import relation_diagnostics
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
    *,
    settings: Mapping[str, Setting] = MappingProxyType({}),
    variable_origins: tuple[Origin, ...] = (),
) -> SemanticView:
    """Build one view from its node and the members attached to it, one phase at a time.

    The phases run in this order, and each raises `ProjectError` at the first problem it finds,
    so the order decides which problem a broken view reports: select the attached members the
    view's scope admits, set up the context, then tables, columns, metric names, metrics and their windows, filters,
    instructions, verified queries, variables, tags, and assemble. A phase reads only what the
    earlier phases returned.

    Args:
        settings: The view's resolved settings, which give its tags and `max_staleness`.
        variable_origins: Where each of the view's variables is written, which its diagnostics name.

    Raises:
        ProjectError: A table, column, member, variable or tag does not resolve. Its diagnostics
            name the problem; one without diagnostics is reported by its message.
    """
    name = str(node["name"])
    scope = view_scope(node)
    selected = _scoped(_select_members(view_key(name), members, attachment), scope)
    view = _View(
        name,
        path,
        Origin(path.resolve().relative_to(project_dir.resolve()).as_posix()),
        models,
        DbtCatalog("v12", None, None, tuple(models.values()), sources=sources),
        mapping(config.get("vars")),
        variable_origins,
    )
    tables, logical_by_model = _view_tables(node, view)
    # A `source()` table has no model; its relation is the source's own, so it cannot be misplaced.
    relations = {logical: models[model].relation_name for model, logical in logical_by_model.items() if model in models}
    misplaced = relation_diagnostics(tables, relations, artifact=view.key)
    if misplaced:
        raise ProjectError(misplaced[0].message, diagnostics=misplaced)
    columns = [column for column in _view_columns(models, logical_by_model) if scope.admits_column(column)]
    resolver = _member_resolver(view, logical_by_model, selected)
    metrics = _view_metrics(selected.metrics, selected.relationships, resolver)
    entity_filters, standalone_filters = _view_filters(selected.filters, resolver)
    instructions = _listed_instructions(node, selected.instructions)
    sql_parts, question_parts = _instruction_parts(instructions, standalone_filters, resolver)
    queries = _view_verified_queries(
        selected.verified_queries, logical_by_model, resolver.metric_names, config, view.catalog
    )
    variables = _view_variables(node, view)
    # Rewrites the resolved expressions, so it runs once the metrics and filters are built.
    metrics, entity_filters = _with_variable_names(variables, metrics, entity_filters)
    tags = _view_tags(setting_value(settings, "tags"), view, config, target)
    max_staleness = setting_value(settings, "max_staleness")
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
        scope=scope,
        member_origins=MappingProxyType({member.key: member.origin for member in selected.attached}),
        settings=tuple(settings[key] for key in sorted(settings)),
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


def _scoped(selected: _Members, scope: ViewScope) -> _Members:
    """Keep the attached metrics and relationships the view's scope admits.

    The metrics are narrowed before any expression resolves, so a metric that reads one the
    scope removed fails to resolve rather than rendering a reference the view cannot satisfy.
    """
    return replace(
        selected,
        metrics=tuple(metric for metric in selected.metrics if scope.admits_metric(metric.name)),
        relationships=tuple(item for item in selected.relationships if scope.admits_relationship(item.name)),
    )


def _listed_instructions(node: Mapping[str, Any], attached: Mapping[str, InstructionDef]) -> tuple[InstructionDef, ...]:
    """Return the attached custom instructions in the order the view's `custom_instructions` lists them.

    Their text joins in this order, so it is the author's, not the order the files were read in.
    An instruction attached and not listed, which loading never produces, follows by name.
    """
    order: list[str] = []
    raw = node.get("custom_instructions")
    for entry in raw if isinstance(raw, list) else []:
        # Read as `_view_instructions` reads it, which reports an entry that is not one call.
        found = required_call(str(entry), "custom_instructions", None, field="custom_instructions", artifact="")
        if isinstance(found, TemplateCall) and found.args[0].casefold() in attached:
            order.append(found.args[0].casefold())
    names = [*dict.fromkeys(order), *sorted(name for name in attached if name not in order)]
    return tuple(attached[name] for name in names)


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
        variable_origins: Where each entry of the view's `variables:` is written, in order.
    """

    name: str
    path: Path
    origin: Origin
    models: Mapping[str, DbtModel]
    catalog: DbtCatalog
    variables: Mapping[str, object]
    variable_origins: tuple[Origin, ...] = ()

    @property
    def key(self) -> str:
        """The view's artifact key, which the view's build diagnostics name."""
        return view_key(self.name)

    def variable_origin(self, index: int) -> Origin:
        """Return where the view's variable at `index` is written; the view's file when unknown."""
        return self.variable_origins[index] if index < len(self.variable_origins) else self.origin


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

    Raises:
        ProjectError: A table entry is not a one-argument `ref()`, or names no model or one an
            earlier entry names.
    """
    # Tables keep DECLARATION order -- that is authored information and the golden
    # preserves it. The renderer sorts relationships, facts, dimensions and metrics;
    # variables, tags and verified queries keep the order they are built in.
    tables: list[Table] = []
    logical_by_model: dict[str, str] = {}
    for raw in node.get("tables") or []:
        model_key, table = _view_table(raw, view, logical_by_model)
        logical_by_model[model_key] = table.logical_name
        tables.append(table)
    return tuple(tables), logical_by_model


def _view_table(raw: object, view: _View, logical_by_model: Mapping[str, str]) -> tuple[str, Table]:
    """Build one table entry against the dbt models and the entries declared before it.

    Its synonyms, COMMENT and `distinct_range` are the model's: one set for every view that uses it.

    Diagnostics:
        SST-DBT011, as `_source_table` reports it, when the entry is one `{{ source() }}` call.
        SST-REF044: when the entry is not one `{{ ref('<model>') }}` call.
        SST-REF001: when the entry names no dbt model.
        SST-PRS006: when the entry names a model an earlier entry names.

    Raises:
        ProjectError: For each diagnostic above.
    """
    source = single_template_call(str(raw), "source")
    if source is not None and len(source.args) == 2:
        return _source_table(source.args, view, logical_by_model)
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
    distinct_range = None
    if model.distinct_range is not None:
        start, end = _names(_column_names(model, model.distinct_range), view.key, None)
        distinct_range = (start, end)
    (logical,) = _names((logical,), view.key, None)
    if qualified_name_problem(model.relation_name, artifact=view.key, subject=view.key) is not None:
        diagnostic = D("SST-REF019", value=model.relation_name, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return model_name.lower(), Table(
        logical_name=logical,
        fqn=model.relation_name,
        primary_key=_names(_column_names(model, model.primary_key), view.key, None),
        unique_keys=tuple(_names(_column_names(model, key), view.key, None) for key in model.unique_keys),
        synonyms=model.synonyms,
        comment=model.description,
        distinct_range=distinct_range,
    )


def _column_names(model: DbtModel, names: tuple[str, ...]) -> tuple[str, ...]:
    """Write each of a model's key columns as a member names it, quoted when dbt quotes it."""
    columns = (model.column(name) for name in names)
    return tuple(name if column is None else column.member_name for name, column in zip(names, columns, strict=True))


def _source_table(args: tuple[str, ...], view: _View, logical_by_model: Mapping[str, str]) -> tuple[str, Table]:
    """Build a table from one `{{ source('<source>', '<table>') }}` entry: a relation without columns.

    Its synonyms and COMMENT are the source table's.

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
    relation = found.relation_name.upper()
    _require(qualified_name_problem(relation, artifact=view.key, subject=view.key))
    return pair.casefold(), Table(
        logical_name=logical,
        fqn=relation,
        primary_key=(),
        unique_keys=(),
        synonyms=found.synonyms,
        comment=found.description,
        distinct_range=None,
    )


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
    (name,) = _names((column.member_name,), subject, None)
    reference = column_ref(Identifier.parse(logical), column.identifier).text
    return Column(
        table=logical,
        name=name,
        kind=kind,
        expr=_expression(reference, kind=kind.value, name=name, subject=subject, origin=None),
        comment=column.description,
        synonyms=column.synonyms,
        sample_values=column.sample_values,
        is_enum=column.is_enum is True,
        data_type=column.data_type,
    )


def _view_variables(node: Mapping[str, Any], view: _View) -> tuple[Variable, ...]:
    """Build the view's variables in declaration order, each name once whatever its case.

    Diagnostics:
        SST-PRS006: when a variable's name, compared without case, repeats an earlier one's.
        Every code `_variable` lists, for each variable.

    Raises:
        ProjectError: A variable is malformed, repeats a name, or its default does not fit its type.
    """
    variables: list[Variable] = []
    seen: set[str] = set()
    for index, value in enumerate(node.get("variables") or []):
        variable = _variable(value, view=view, origin=view.variable_origin(index))
        # Snowflake folds an unquoted name to upper case, and refuses one declared twice.
        if variable.name.casefold() in seen:
            diagnostic = D("SST-PRS006", type="variable", name=str(value["name"]), origin=view.variable_origin(index))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        seen.add(variable.name.casefold())
        variables.append(variable)
    return tuple(variables)


def _variable(value: object, *, view: _View, origin: Origin) -> Variable:
    """Build one view variable, its default rendered as a constant of its declared type.

    The type is checked first, then the default against it (see `typed_default`). A YAML date or
    timestamp, which the parser reads as one, is checked as the ISO text it was written as.

    Diagnostics:
        SST-PRS003: when the data type is not a Snowflake data type.
        SST-PRS127: when the default is not a value of the data type.

    Raises:
        ProjectError: The variable lacks a name, a type or a default, or for each diagnostic above.
    """
    if (
        not isinstance(value, dict)
        or not value.get("name")
        or not value.get("data_type")
        or "default_value" not in value
    ):
        raise ProjectError(f"{view.path}: view {view.name} has an invalid variable {value!r}")
    data_type = str(value["data_type"]).upper()
    subject = view.key
    if not is_datatype(data_type):
        diagnostic = D(
            "SST-PRS003",
            artifact=subject,
            field=f"variables.{value['name']}.data_type",
            expected="a Snowflake data type",
            found=repr(value["data_type"]),
            subject=subject,
            origin=origin,
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    raw_default = value["default_value"]
    if isinstance(raw_default, (date, datetime)):
        raw_default = str(raw_default)
    try:
        default = typed_default(data_type, raw_default)
    except DefaultMismatch as exc:
        diagnostic = D(
            "SST-PRS127",
            artifact=subject,
            name=str(value["name"]),
            type=data_type,
            found=repr(raw_default),
            expected=exc.expected,
            subject=subject,
            origin=origin,
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    (name,) = _names((str(value["name"]),), subject, origin)
    return Variable(
        name=name,
        data_type=datatype(data_type),
        default=default,
        comment=str(value.get("description") or "").strip() or None,
    )


def _view_tags(raw_tags: object, view: _View, config: dict[str, Any], target: DbtTarget) -> tuple[Tag, ...]:
    """Build the view's resolved tags in declaration order.

    Diagnostics:
        SST-PRS027: when the tags are not a list.

    Raises:
        ProjectError: the tags are not a list, or a tag does not build (see `_tag`).
    """
    if raw_tags is not None and not isinstance(raw_tags, list):
        diagnostic = D("SST-PRS027", artifact=view.key, found=type(raw_tags).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    declared = declared_tags(config.get("tags"), target)
    return tuple(_tag(value, view=view, tags=declared) for value in raw_tags or [])


def _tag(value: object, *, view: _View, tags: DeclaredTags) -> Tag:
    """Build one tag: its name one `tag()` call naming a tag under `tags:` in `sst_config.yml`.

    Diagnostics:
        SST-PRS027: when the entry lacks a name or a value.
        Every code `tag_reference` lists, for a name that is not one `tag()` call naming a
            declared tag that can be qualified.
        SST-REF019: when the tag's name is not a three-part name.
        SST-PRS003: when the value is not text or a number, such as an unquoted `no`.
        SST-PRS026: when the value is longer than 256 characters.

    Raises:
        ProjectError: For each diagnostic above.
    """
    if not isinstance(value, dict) or not value.get("name") or "value" not in value:
        diagnostic = D("SST-PRS027", artifact=view.key, found=repr(value))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    resolved = tag_reference(
        str(value["name"]),
        tags,
        view.origin,
        reference="semantic_view.config.tags.name",
        field="config.tags.name",
        artifact=view.key,
        subject=view.key,
    )
    if isinstance(resolved, Diagnostic):
        raise ProjectError(resolved.message, diagnostics=(resolved,))
    if qualified_name_problem(resolved, artifact=view.key, subject=view.key) is not None:
        diagnostic = D("SST-REF019", value=resolved, origin=view.origin, subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    written = value["value"]
    if not isinstance(written, (str, int, float)) or isinstance(written, bool):
        # An unquoted `no` is YAML 1.1's false; published as text it would read 'False'.
        found = type(written).__name__
        diagnostic = D(
            "SST-PRS003",
            artifact=view.key,
            field="config.tags.value",
            expected="a string",
            found=found,
            subject=view.key,
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    text = str(written)
    if len(text) > 256:
        call = single_template_call(str(value["name"]), "tag")
        field = call.args[0] if call is not None and call.args else str(value["name"])
        diagnostic = D("SST-PRS026", artifact=view.key, field=field, size=len(text), subject=view.key)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return Tag(name=resolved, value=text)


def _source_files(
    path: Path,
    project_dir: Path,
    attached: tuple[ParsedMember, ...],
    models: Mapping[str, DbtModel],
    logical_by_model: Mapping[str, str],
) -> tuple[str, tuple[str, ...]]:
    """Return the view file's project-relative path, and every file the view is built from, sorted.

    Those are the view's own file, each attached member's, the file each attached verified
    query loads its SQL from, and each of its models' dbt files.
    """
    source_path = path.resolve().relative_to(project_dir.resolve()).as_posix()
    source_files = {source_path}
    source_files.update(member.origin.file for member in attached)
    source_files.update(
        member.source.sql_file
        for member in attached
        if isinstance(member.source, VerifiedQueryDef) and member.source.sql_file is not None
    )
    for model_key in logical_by_model:
        model = models.get(model_key)
        model_path = (model.patch_path or model.original_file_path) if model is not None else None
        if model_path:
            source_files.add(model_path)
    return source_path, tuple(sorted(source_files))
