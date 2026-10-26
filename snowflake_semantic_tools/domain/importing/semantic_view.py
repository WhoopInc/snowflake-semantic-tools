"""Map a live semantic view back to the project files and dbt model metadata that would publish it.

The inverse of `domain.render.semantic_view`. A 1.0 view lists only its tables, by `ref()`; its
columns come from each dbt model's `config.meta.sst`, and its metrics, relationships, filters,
verified queries and custom instructions are files of their own that attach by the tables they
name. So a live view becomes:

- its view file, with its tables, variables, settings and custom instruction blocks;
- an edit to each table's dbt model, setting the keys the project leaves unset; a key the
  project sets to another value is kept, and reported (SST-IMP011);
- one file per member the project does not declare already, each expression rewritten with
  `ref()` and `metric()` calls (`expressions.rewrite`).

A table that is no dbt model is SST-IMP010, and the view is not imported. A fact or dimension
computed from an expression has no 1.0 form, since columns come from dbt, and is reported
(SST-IMP013), as is anything else SST would publish otherwise.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.importing.expressions import (
    ViewNames,
    parse_window,
    rewrite,
    rewrite_tables,
    split_window,
)
from snowflake_semantic_tools.domain.importing.model import (
    ColumnMeta,
    ImportedFile,
    ImportedObject,
    ModelEdit,
    artifact_name,
)
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.references import ProjectIndex
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle.marker import unmarked_comment
from snowflake_semantic_tools.domain.sql.lexer import LexError

_COLUMN_TYPES = (("facts", "fact"), ("dimensions", "dimension"), ("time_dimensions", "time_dimension"))


@dataclass(frozen=True, slots=True)
class ViewPlacement:
    """Where a view's files go, and what it inherits there.

    Attributes:
        root: `project.semantic_models_dir`.
        destination: The folder below `root`; its members go in subfolders of it.
        inherited: Each `semantic_views:` setting the folder's routes give, rendered.
    """

    root: str
    destination: Destination
    inherited: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


class _View:
    """One view's mapping: the files and edits it adds, and what it reports."""

    def __init__(self, name: QualifiedName, index: ProjectIndex, placement: ViewPlacement) -> None:
        self.live = name
        self.index = index
        self.placement = placement
        self.name = artifact_name(name)
        self.key = f"semantic_view:{self.name}"
        self.folder = posixpath.join(placement.root, *placement.destination.folder)
        self.files: list[ImportedFile] = []
        self.diagnostics: list[Diagnostic] = []
        self.edits: dict[str, ModelEdit] = {}
        self.tables: dict[str, str] = {}

    def differs(self, detail: str) -> None:
        self.diagnostics.append(D("SST-IMP013", subject=self.key, artifact=self.key, detail=detail))

    def member(self, kind: str, folder: str, name: str, root: str, body: Mapping[str, object]) -> None:
        """Add one member's file, unless the project declares a member of that name already."""
        if self.index.declares(kind, name):
            return
        path = posixpath.join(self.folder, folder, f"{file_name(name)}.yml")
        self.files.append(ImportedFile(path, {root: dict(body)}))


def import_semantic_view(
    name: QualifiedName, document: Mapping[str, Any], index: ProjectIndex, placement: ViewPlacement
) -> ImportedObject:
    """Map a live view's `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` document to project files and model edits.

    Diagnostics:
        SST-IMP010: a table is no dbt model of the manifest; the view is not imported.
        SST-IMP011: a dbt model or column sets a key to another value than the live view.
        SST-IMP013: a member SST cannot write as Snowflake holds it.
        SST-IMP015: sample values of PII-tagged columns were dropped.
    """
    view = _View(name, index, placement)
    tables = list(_items(document.get("tables")))
    refs = [reference for table in tables if (reference := _table_ref(view, table)) is not None]
    if any(item.blocks for item in view.diagnostics):
        return ImportedObject("semantic_view", name, view.name, diagnostics=tuple(view.diagnostics))
    names = _names(view, document)
    for table in tables:
        _table(view, table, names)
    for item in _items(document.get("relationships")):
        _relationship(view, item, names)
    for table in tables:
        for item in _items(table.get("metrics")):
            _metric(view, item, names)
    for item in _items(document.get("metrics")):
        _metric(view, item, names)
    for item in _items(document.get("verified_queries")):
        _verified_query(view, item, names)
    body: dict[str, object] = {"name": view.name}
    description = unmarked_comment(str(document.get("description") or ""))
    if description:
        body["description"] = description
    body["tables"] = refs
    variables = [_variable(item) for item in _items(document.get("variables"))]
    if variables:
        body["variables"] = variables
    body.update(_column_scope(view, document))
    _reach(view)
    config = _config(view, str(document.get("max_staleness") or ""))
    if config:
        body["config"] = config
    blocks = _instructions(view, document)
    if blocks:
        body["custom_instructions"] = blocks
    main = ImportedFile(posixpath.join(view.folder, f"{file_name(view.name)}.yml"), {"semantic_view": body})
    return ImportedObject(
        "semantic_view",
        name,
        view.name,
        (main, *view.files),
        model_edits=tuple(edit for edit in view.edits.values() if edit.meta or edit.columns or edit.description),
        diagnostics=tuple(view.diagnostics),
    )


def _table_ref(view: _View, table: Mapping[str, Any]) -> str | None:
    """Return the `ref()` or `source()` a table is written as, or report that it is neither."""
    logical = str(table.get("name") or "")
    base = _entry(table.get("base_table"))
    parts = tuple(str(base.get(key) or "") for key in ("database", "schema", "table"))
    try:
        relation = QualifiedName(*(Identifier.shown(part) for part in parts))
    except ValueError:
        relation = None
    model = view.index.model(relation) if relation is not None else None
    if model is not None:
        view.tables[logical.upper()] = model
        if logical.upper() != model.upper():
            view.differs(
                f"table {logical} is written as its model, {model}, which names it {model.upper()} in the view"
            )
        return "{{ ref('" + model + "') }}"
    source = view.index.sources.get(relation.folded) if relation is not None else None
    if source is not None:
        view.tables[logical.upper()] = ""
        return "{{ source('" + source[0] + "', '" + source[1] + "') }}"
    value = relation.sql if relation is not None else ".".join(parts)
    view.diagnostics.append(
        D("SST-IMP010", subject=view.key, artifact=view.key, value=value, target=view.index.target_name)
    )
    return None


def _names(view: _View, document: Mapping[str, Any]) -> ViewNames:
    """Return the names the view's expressions use: its tables' models, their columns, and its metrics."""
    columns: dict[str, dict[str, str]] = {}
    for model in view.tables.values():
        found = view.index.dbt_models.get(model)
        if found is not None:
            columns[model] = {column.name.upper(): column.name for column in found.columns}
    table_metrics: dict[tuple[str, str], str] = {}
    metrics: dict[str, str] = {}
    for table in _items(document.get("tables")):
        for item in _items(table.get("metrics")):
            metric = str(item.get("name") or "")
            table_metrics[(str(table.get("name") or "").upper(), metric.upper())] = metric.casefold()
            metrics[metric.upper()] = metric.casefold()
    for item in _items(document.get("metrics")):
        metric = str(item.get("name") or "")
        metrics[metric.upper()] = metric.casefold()
    return ViewNames(
        MappingProxyType({logical: model for logical, model in view.tables.items() if model}),
        MappingProxyType(columns),
        MappingProxyType(table_metrics),
        MappingProxyType(metrics),
    )


def _table(view: _View, table: Mapping[str, Any], names: ViewNames) -> None:
    """Map a table's keys, synonyms and comment, and its facts and dimensions, onto its dbt model."""
    logical = str(table.get("name") or "")
    model_name = view.tables.get(logical.upper(), "")
    model = view.index.dbt_models.get(model_name)
    if model is None:
        return
    meta: list[tuple[str, object]] = []
    key = _strings(_entry(table.get("primary_key")).get("columns"))
    _model_key(view, model, meta, "primary_key", _columns(names, logical, key), model.primary_key)
    unique = tuple(_columns(names, logical, _strings(item.get("columns"))) for item in _items(table.get("unique_keys")))
    _model_key(view, model, meta, "unique_keys", unique, model.unique_keys)
    _model_key(view, model, meta, "synonyms", _strings(table.get("synonyms")), model.synonyms)
    for constraint in _items(table.get("constraints")):
        span = _entry(constraint.get("distinct_range"))
        if span:
            start = names.column(logical, str(span.get("start_column")))
            end = names.column(logical, str(span.get("end_column")))
            if model.distinct_range is None:
                meta.append(("distinct_range", {"start": start, "end": end}))
            else:
                _model_key(view, model, meta, "distinct_range", (start, end), model.distinct_range)
    description = _kept(
        view, model.name, "(model)", "description", str(table.get("description") or ""), model.description
    )
    columns: list[ColumnMeta] = []
    dropped = 0
    for section, column_type in _COLUMN_TYPES:
        for item in _items(table.get(section)):
            if "filter" in {str(label).casefold() for label in _strings(item.get("labels"))}:
                _filter(view, item, names)
                continue
            mapped, pii = _column(view, model, logical, item, column_type)
            dropped += pii
            if mapped is not None:
                columns.append(mapped)
    if dropped:
        view.diagnostics.append(D("SST-IMP015", subject=view.key, artifact=view.key, count=dropped))
    view.edits[model.name] = ModelEdit(
        model.name,
        model.patch_file or _new_patch_file(model),
        model.resource_type + "s" if model.resource_type in ("snapshot", "seed") else "models",
        tuple(meta),
        description,
        tuple(columns),
    )


def _column(
    view: _View, model: DbtModel, logical: str, item: Mapping[str, Any], column_type: str
) -> tuple[ColumnMeta | None, int]:
    """Map one fact or dimension onto its dbt column; None for a computed one, which has no 1.0 form."""
    name = str(item.get("name") or "")
    expression = str(item.get("expr") or "")
    # Snowflake shows a plain column as `TABLE.NAME`, or as `NAME` alone for some views.
    plain = " ".join(expression.split()).upper()
    if plain not in (f"{logical}.{name}".upper(), name.upper()):
        view.differs(
            f"{column_type} {logical}.{name} is computed, as {expression}; SST reads facts and dimensions "
            "from dbt columns, so add it to the model as a column"
        )
        return None, 0
    if str(item.get("access_modifier") or "") == "private_access":
        view.differs(f"{column_type} {logical}.{name} is private, which SST does not publish; it is written public")
    column = model.column(name)
    spelled = column.name if column is not None else name.casefold()
    meta: list[tuple[str, object]] = []
    held_type = column.column_type if column is not None and "column_type" in column.declared_keys else None
    # A time dimension renders in DIMENSIONS, as live reads it back: one the project types so is kept.
    if not (held_type == "time_dimension" and column_type == "dimension"):
        _column_key(view, model, column, meta, "column_type", column_type)
    _column_key(view, model, column, meta, "synonyms", _strings(item.get("synonyms")))
    pii = 0
    samples = _strings(item.get("sample_values"))
    if samples and column is not None and column.pii_tagged:
        pii = 1
    else:
        _column_key(view, model, column, meta, "sample_values", samples)
    if item.get("is_enum") is True:
        _column_key(view, model, column, meta, "is_enum", True)
    description = _kept(
        view,
        model.name,
        spelled,
        "description",
        str(item.get("description") or ""),
        column.description if column else None,
    )
    if not meta and description is None:
        return None, pii
    return ColumnMeta(spelled, tuple(meta), description), pii


def _filter(view: _View, item: Mapping[str, Any], names: ViewNames) -> None:
    name = str(item.get("name") or "").casefold()
    expression = _rewritten(view, f"filter {name}", str(item.get("expr") or ""), names)
    if expression is None:
        return
    body: dict[str, object] = {"name": name}
    if item.get("description"):
        body["description"] = item["description"]
    body["expr"] = expression
    body["labels"] = ["filter"]
    view.member("filter", "filters", name, "filter", body)


def _metric(view: _View, item: Mapping[str, Any], names: ViewNames) -> None:
    """Map one metric, a window metric's `OVER (...)` into its `window:` block."""
    name = str(item.get("name") or "").casefold()
    body: dict[str, object] = {"name": name}
    if item.get("description"):
        body["description"] = item["description"]
    expression = str(item.get("expr") or "")
    windowed = split_window(expression)
    if windowed is not None:
        aggregate, inside = windowed
        rewritten = _rewritten(view, f"metric {name}", aggregate, names)
        window = parse_window(inside, names)
        if rewritten is None or window is None:
            view.differs(f"metric {name}'s window, OVER ({inside}), is not one SST can write; it was left out")
            return
        body["expr"] = rewritten
        block: dict[str, object] = {}
        if window.partition_by:
            block["partition_by"] = list(window.partition_by)
        if window.partition_excluding:
            block["partition_by_excluding"] = list(window.partition_excluding)
        if window.order_by:
            block["order_by"] = [dict(entry) for entry in window.order_by]
        if window.frame:
            block["frame"] = window.frame
        body["window"] = block
    else:
        rewritten = _rewritten(view, f"metric {name}", expression, names)
        if rewritten is None:
            return
        body["expr"] = rewritten
    if _strings(item.get("synonyms")):
        body["synonyms"] = list(_strings(item.get("synonyms")))
    using = _strings(item.get("using_relationships"))
    if using:
        body["using_relationships"] = ["{{ relationship('" + entry.casefold() + "') }}" for entry in using]
    non_additive = [_sort_entry(names, entry) for entry in _items(item.get("non_additive_dimensions"))]
    if non_additive:
        body["non_additive_dimensions"] = non_additive
    if str(item.get("access_modifier") or "") == "private_access":
        body["access_modifier"] = "private_access"
    view.member("metric", "metrics", name, "metric", body)


def _relationship(view: _View, item: Mapping[str, Any], names: ViewNames) -> None:
    """Map a relationship's columns to its conditions: `=`, `>=` for an ASOF column, `BETWEEN` for a range."""
    name = str(item.get("name") or "").casefold()
    left, right = str(item.get("left_table") or ""), str(item.get("right_table") or "")
    left_model, right_model = names.tables.get(left.upper()), names.tables.get(right.upper())
    if left_model is None or right_model is None:
        view.differs(f"relationship {name} joins a table that is no dbt model; it was left out")
        return
    conditions: list[str] = []
    for column in _items(item.get("relationship_columns")):
        left_ref = "{{ ref('" + left_model + "', '" + names.column(left, str(column.get("left_column"))) + "') }}"
        if column.get("type") == "range":
            span = _entry(column.get("right_range"))
            start = names.column(right, str(span.get("start_column")))
            end = names.column(right, str(span.get("end_column")))
            bounds = (f"{{{{ ref('{right_model}', '{bound}') }}}}" for bound in (start, end))
            conditions.append(f"{left_ref} BETWEEN " + " AND ".join(bounds))
            continue
        right_ref = "{{ ref('" + right_model + "', '" + names.column(right, str(column.get("right_column"))) + "') }}"
        operator = ">=" if column.get("type") == "asof" else "="
        conditions.append(f"{left_ref} {operator} {right_ref}")
    body = {
        "name": name,
        "left_table": "{{ ref('" + left_model + "') }}",
        "right_table": "{{ ref('" + right_model + "') }}",
        "relationship_conditions": conditions,
    }
    view.member("relationship", "relationships", name, "relationship", body)


def _verified_query(view: _View, item: Mapping[str, Any], names: ViewNames) -> None:
    name = str(item.get("name") or "").casefold()
    try:
        sql = rewrite_tables(str(item.get("sql") or ""), names).text
    except LexError as exc:
        view.differs(f"verified query {name}'s SQL does not lex ({exc.reason}); it was left out")
        return
    body: dict[str, object] = {"name": name, "question": str(item.get("question") or ""), "sql": sql}
    if item.get("use_as_onboarding_question") is True:
        body["use_as_onboarding_question"] = True
    if isinstance(item.get("verified_at"), int):
        body["verified_at"] = item["verified_at"]
    if item.get("verified_by"):
        body["verified_by"] = str(item["verified_by"])
    view.member("verified_query", "verified_queries", name, "verified_query", body)


def _instructions(view: _View, document: Mapping[str, Any]) -> list[str]:
    """List the blocks another view renders the same text from, else write one block named after the view."""
    texts = _entry(document.get("module_custom_instructions"))
    generation = str(texts.get("sql_generation") or "")
    categorization = str(texts.get("question_categorization") or "")
    if not generation and not categorization:
        return []
    shared = view.index.instructions_for(generation, categorization)
    if shared is not None:
        # The blocks another view renders this exact text from: views sharing a table must list
        # the same guidance (SST-MEM105), and these say what the live view says.
        return ["{{ custom_instructions('" + name + "') }}" for name in shared]
    if view.index.declares("custom_instruction", view.name):
        view.differs(f"its custom instructions are the project's block {view.name}, which already exists")
    body: dict[str, object] = {"name": view.name}
    if generation:
        body["ai_sql_generation"] = generation
    if categorization:
        body["ai_question_categorization"] = categorization
    view.member("custom_instruction", "custom_instructions", view.name, "custom_instruction", body)
    return ["{{ custom_instructions('" + view.name + "') }}"]


def _column_scope(view: _View, document: Mapping[str, Any]) -> dict[str, list[str]]:
    """Return the view's column scope, so it publishes the columns the live view has and no more.

    A 1.0 view publishes every typed column of its tables. When the project types a column the
    live view leaves out, the view lists the shorter of what it keeps (`columns:`) and what it
    drops (`exclude_columns:`); empty when it drops nothing.
    """
    live: set[tuple[str, str]] = set()
    for table in _items(document.get("tables")):
        table_model = view.tables.get(str(table.get("name") or "").upper(), "")
        for section, _ in _COLUMN_TYPES:
            live.update(
                (table_model, str(item.get("name") or "").upper())
                for item in _items(table.get(section))
                if "filter" not in {str(label).casefold() for label in _strings(item.get("labels"))}
            )
    kept: list[str] = []
    dropped: list[str] = []
    for model_name in dict.fromkeys(view.tables.values()):
        model = view.index.dbt_models.get(model_name)
        typed = {column.name.upper(): column for column in model.columns} if model is not None else {}
        imported = (
            {meta.column.upper() for meta in view.edits[model_name].columns} if model_name in view.edits else set()
        )
        for name in dict.fromkeys((*typed, *imported)):
            column = typed.get(name)
            reference = "{{ ref('" + model_name + "', '" + (column.name if column else name.casefold()) + "') }}"
            if (model_name, name) in live:
                kept.append(reference)
            elif column is not None and column.column_type and not column.excluded:
                dropped.append(reference)
    if not dropped:
        return {}
    return {"columns": kept} if len(kept) < len(dropped) else {"exclude_columns": dropped}


def _reach(view: _View) -> None:
    """Report the other views whose tables the import types columns on or adds members over.

    A member attaches to every view holding its tables, and a typed column to every view holding
    its model, so those views publish them at their next apply too.
    """
    edited = {edit.model for edit in view.edits.values() if edit.columns}
    touched = edited | ({model for model in view.tables.values() if model} if view.files else set())
    others = sorted(name for name, models in view.index.view_models.items() if name != view.name and models & touched)
    if others:
        view.differs(
            "a member or column it adds attaches to every view over its tables, so "
            + ", ".join(f"semantic_view:{name}" for name in others)
            + " publish them too at their next apply; exclude them there, or review with `sst plan`"
        )


def _variable(item: Mapping[str, Any]) -> dict[str, object]:
    data_type = str(item.get("data_type") or "").split("(", 1)[0].strip().upper()
    default: object = item.get("default_value")
    if isinstance(default, str):
        if data_type == "BOOLEAN" and default.casefold() in ("true", "false"):
            default = default.casefold() == "true"
        elif data_type in ("NUMBER", "INT", "INTEGER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"):
            default = _number(default)
    entry: dict[str, object] = {"name": str(item.get("name") or "").casefold(), "data_type": data_type}
    if item.get("description"):
        entry["description"] = item["description"]
    entry["default_value"] = default
    return entry


def _config(view: _View, staleness: str) -> dict[str, object]:
    config: dict[str, object] = {}
    if view.placement.destination.overridden:
        database, schema, _ = view.live.folded
        config.update({"database": database, "schema": schema})
    seconds = _seconds(staleness)
    inherited = view.placement.inherited.get("max_staleness")
    if seconds is not None and str(inherited) != str(seconds):
        config["max_staleness"] = seconds
    elif seconds is None and inherited is not None:
        view.differs(
            f"its folder sets max_staleness {inherited}, which the live view does not set; the next apply sets it"
        )
    return config


def _rewritten(view: _View, label: str, expression: str, names: ViewNames) -> str | None:
    try:
        return rewrite(expression, names).text
    except LexError as exc:
        view.differs(f"{label}'s expression does not lex ({exc.reason}); it was left out")
        return None


def _sort_entry(names: ViewNames, entry: Mapping[str, Any]) -> dict[str, object]:
    table, dimension = str(entry.get("table") or ""), str(entry.get("dimension") or "")
    model = names.tables.get(table.upper(), table.casefold())
    written: dict[str, object] = {"ref": "{{ ref('" + model + "', '" + names.column(table, dimension) + "') }}"}
    if entry.get("sort_direction"):
        written["sort_direction"] = str(entry["sort_direction"]).casefold()
    if entry.get("null_order"):
        written["null_order"] = str(entry["null_order"]).casefold()
    return written


def _model_key(view: _View, model: DbtModel, meta: list[tuple[str, object]], key: str, live: Any, held: Any) -> None:
    """Set a model-level key the project leaves unset; report one it sets to another value."""
    if not live:
        return
    if not held:
        meta.append((key, live))
    elif _normal(held) != _normal(live):
        view.diagnostics.append(
            D(
                "SST-IMP011",
                subject=view.key,
                artifact=view.key,
                model=model.name,
                column="(model)",
                key=key,
                found=held,
                expected=live,
            )
        )


def _column_key(
    view: _View, model: DbtModel, column: DbtColumn | None, meta: list[tuple[str, object]], key: str, live: Any
) -> None:
    """Set a column key the project leaves unset; report one it sets to another value."""
    if live in ((), "", None):
        return
    held = getattr(column, key, None) if column is not None and key in column.declared_keys else None
    if held in (None, (), ""):
        meta.append((key, live))
    elif _normal(held) != _normal(live):
        name = column.name if column is not None else ""
        view.diagnostics.append(
            D(
                "SST-IMP011",
                subject=view.key,
                artifact=view.key,
                model=model.name,
                column=name,
                key=key,
                found=held,
                expected=live,
            )
        )


def _kept(view: _View, model: str, column: str, key: str, live: str, held: str | None) -> str | None:
    """Return a description to write where the project has none; report one that differs."""
    if not live:
        return None
    if not held:
        return live
    if " ".join(held.split()) != " ".join(live.split()):
        view.diagnostics.append(
            D(
                "SST-IMP011",
                subject=view.key,
                artifact=view.key,
                model=model,
                column=column,
                key=key,
                found=held,
                expected=live,
            )
        )
    return None


def _columns(names: ViewNames, logical: str, columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(names.column(logical, column) for column in columns)


def _new_patch_file(model: DbtModel) -> str:
    """The YAML file an import creates for a model the project documents nowhere: beside its SQL."""
    source = model.original_file_path or f"models/{model.name}.sql"
    return posixpath.join(posixpath.dirname(source), f"{file_name(model.name)}.yml")


def _normal(value: object) -> object:
    if isinstance(value, str):
        return value.casefold()
    if isinstance(value, list | tuple):
        return tuple(_normal(item) for item in value)
    return value


def _seconds(text: str) -> int | None:
    parts = text.split()
    if len(parts) == 2 and parts[0].isdigit() and parts[1].casefold().startswith("second"):
        return int(parts[0])
    if len(parts) == 1 and parts[0].isdigit():
        return int(parts[0])
    return None


def _number(text: str) -> object:
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return text


def _items(value: object) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in value if isinstance(item, Mapping)) if isinstance(value, list) else ()


def _entry(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in value) if isinstance(value, list) else ()
