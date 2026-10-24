"""A semantic view's definition: what SST renders for it, and what Snowflake shows of the live one.

Every side lists the same members under the same names: each table with its base table, keys,
synonyms and comment; each distinct-range constraint; each relationship; each fact, dimension,
filter and metric by its qualified name; each variable; each verified query; the two custom
instruction texts; the view's comment without its ownership marker; and its staleness. The
desired side holds only what the view's scope admits, as the renderer does.

The live side is read from `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` (`live_view_from_yaml`), which
reports all of it, a window metric's expression, `USING`, and ASOF and range relationships
included, as measured on a live account. Where that function cannot be called, `DESCRIBE SEMANTIC VIEW`
is read instead (`live_view`). It has not been seen to report variables or the staleness, so its
definition declares it did not read those kinds, which are then not compared; nor the properties
it does not report, which only the other sides state.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle.marker import unmarked_comment
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    SemanticView,
    SortKey,
    Table,
)
from snowflake_semantic_tools.domain.plan.member_diff import Definition, Member, Value
from snowflake_semantic_tools.domain.ports.snowflake.catalog import DescribedMember
from snowflake_semantic_tools.domain.render.semantic_view import render_window

# The order members are listed in, by kind.
_ORDER = (
    "table",
    "constraint",
    "relationship",
    "fact",
    "dimension",
    "filter",
    "metric",
    "variable",
    "verified_query",
    "instruction",
    "view",
    "setting",
)
# The kinds DESCRIBE has not been seen to report, which a definition read from it does not compare.
_UNDESCRIBED = frozenset(("variable", "setting"))
_COLUMN_KINDS = {
    ColumnKind.FACT: "fact",
    ColumnKind.DIMENSION: "dimension",
    ColumnKind.TIME_DIMENSION: "dimension",
    ColumnKind.FILTER: "filter",
}
_INSTRUCTIONS = (("AI_SQL_GENERATION", "sql_generation"), ("AI_QUESTION_CATEGORIZATION", "question_categorization"))


def desired_view(view: SemanticView) -> Definition:
    """Return the definition SST renders for `view`: the members its scope admits."""
    scope = view.scope
    members: list[Member] = []
    for table in view.tables:
        members.append(_table(table))
        if table.distinct_range is not None:
            start, end = table.distinct_range
            members.append(_constraint(f"{table.logical_name}_DISTINCT_RANGE", "DISTINCT_RANGE", start, end))
    for relationship in view.relationships:
        if not scope.admits_relationship(relationship.name):
            continue
        properties: list[tuple[str, Value]] = [
            ("table", _display(relationship.from_table)),
            ("columns", _upper(relationship.from_columns)),
            ("ref_table", _display(relationship.to_table)),
        ]
        if relationship.asof_index is None and relationship.range_bounds is None:
            properties.append(("ref_columns", _upper(relationship.to_columns)))
        asof = relationship.to_columns[relationship.asof_index] if relationship.asof_index is not None else ""
        ranged = " AND ".join(relationship.range_bounds) if relationship.range_bounds is not None else ""
        properties += [("asof_column", asof.upper()), ("range", ranged.upper())]
        members.append(Member("relationship", _display(relationship.name), tuple(properties)))
    members.extend(_column(column) for column in view.columns if scope.admits_column(column))
    members.extend(_metric(metric) for metric in view.metrics if scope.admits_metric(metric.name))
    members.extend(
        _variable(variable.name, str(variable.data_type), str(variable.default), variable.comment or "")
        for variable in view.variables
    )
    for query in view.verified_queries:
        members.append(
            Member(
                "verified_query",
                _display(query.name),
                (
                    ("question", query.question),
                    ("sql", query.sql.text),
                    ("verified_at", "" if query.verified_at is None else str(query.verified_at)),
                    ("verified_by", query.verified_by or ""),
                    ("onboarding_question", "true" if query.onboarding_question else ""),
                ),
            )
        )
    members.append(Member("instruction", "sql_generation", (("text", view.ai_sql_generation or ""),)))
    members.append(Member("instruction", "question_categorization", (("text", view.ai_question_categorization or ""),)))
    members.append(Member("view", "comment", (("text", unmarked_comment(view.comment)),)))
    members.append(_staleness(view.max_staleness or ""))
    return Definition(_sorted(members))


def live_view(rows: Iterable[DescribedMember]) -> Definition:
    """Return the definition `DESCRIBE SEMANTIC VIEW` shows, from its rows.

    The variables and the staleness, which DESCRIBE does not report, are kinds it did not read.
    """
    grouped: dict[tuple[str | None, str | None, str | None], dict[str, list[str]]] = {}
    for row in rows:
        values = grouped.setdefault((row.kind, row.parent, row.name), {})
        values.setdefault(row.property.upper(), []).append(row.value or "")
    members: list[Member] = []
    instructions: dict[str, str] = {}
    comment = ""
    for (kind, parent, name), values in grouped.items():
        member = _live_member(kind, parent, name or "", values)
        if member is not None:
            members.append(member)
        elif kind == "CUSTOM_INSTRUCTION":
            instructions.update({key: texts[0] for key, texts in values.items()})
        elif kind is None:
            comment = unmarked_comment(_first(values, "COMMENT"))
    members.extend(
        Member("instruction", label, (("text", instructions.get(prop, "")),)) for prop, label in _INSTRUCTIONS
    )
    members.append(Member("view", "comment", (("text", comment),)))
    return Definition(_sorted(members), frozenset(_ORDER) - _UNDESCRIBED)


def live_view_from_yaml(document: Mapping[str, Any]) -> Definition:
    """Return the definition `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` shows, from its parsed document.

    Raises:
        ValueError: the document names one member twice, or is not a semantic view's.
    """
    if not isinstance(document.get("tables"), list):
        raise ValueError("the document holds no `tables` list")
    members: list[Member] = []
    for table in _items(document.get("tables")):
        name = str(table.get("name") or "")
        members.append(_yaml_table(table))
        members.extend(
            _constraint(
                str(item.get("name") or ""),
                "DISTINCT_RANGE",
                str(_entry(item.get("distinct_range")).get("start_column") or ""),
                str(_entry(item.get("distinct_range")).get("end_column") or ""),
            )
            for item in _items(table.get("constraints"))
            if isinstance(item.get("distinct_range"), Mapping)
        )
        for section in ("facts", "dimensions", "time_dimensions"):
            members.extend(_yaml_column(section, name, item) for item in _items(table.get(section)))
        members.extend(_yaml_metric(f"{name}.{item.get('name')}", item) for item in _items(table.get("metrics")))
    members.extend(_yaml_relationship(item) for item in _items(document.get("relationships")))
    members.extend(_yaml_metric(str(item.get("name") or ""), item) for item in _items(document.get("metrics")))
    members.extend(
        _variable(
            str(item.get("name") or ""),
            str(item.get("data_type") or ""),
            str(item.get("default_value") if item.get("default_value") is not None else ""),
            str(item.get("description") or ""),
        )
        for item in _items(document.get("variables"))
    )
    members.extend(_yaml_query(item) for item in _items(document.get("verified_queries")))
    instructions = _entry(document.get("module_custom_instructions"))
    members.extend(
        Member("instruction", label, (("text", str(instructions.get(label) or "")),)) for _, label in _INSTRUCTIONS
    )
    members.append(Member("view", "comment", (("text", unmarked_comment(str(document.get("description") or ""))),)))
    members.append(_staleness(str(document.get("max_staleness") or "")))
    return Definition(_sorted(members))


def _yaml_table(table: Mapping[str, Any]) -> Member:
    base = _entry(table.get("base_table"))
    parts = tuple(str(base.get(key) or "") for key in ("database", "schema", "table"))
    qualified = QualifiedName(*(Identifier.shown(part) for part in parts)).sql if all(parts) else ""
    return Member(
        "table",
        _display(str(table.get("name") or "")),
        (
            ("base_table", qualified),
            ("primary_key", _upper(_strings(_entry(table.get("primary_key")).get("columns")))),
            (
                "unique_keys",
                tuple(", ".join(_upper(_strings(key.get("columns")))) for key in _items(table.get("unique_keys"))),
            ),
            ("synonyms", _strings(table.get("synonyms"))),
            ("comment", str(table.get("description") or "")),
        ),
    )


def _yaml_column(section: str, table: str, item: Mapping[str, Any]) -> Member:
    labels = {str(label).casefold() for label in _strings(item.get("labels"))}
    kind = "filter" if "filter" in labels else "fact" if section == "facts" else "dimension"
    properties: list[tuple[str, Value]] = [
        ("expression", str(item.get("expr") or "")),
        ("synonyms", _strings(item.get("synonyms"))),
        ("comment", str(item.get("description") or "")),
        ("sample_values", _strings(item.get("sample_values"))),
    ]
    if kind == "dimension":
        properties.append(("is_enum", "true" if item.get("is_enum") is True else ""))
    return Member(kind, _display(f"{table}.{item.get('name')}"), tuple(properties))


def _yaml_metric(qualified: str, item: Mapping[str, Any]) -> Member:
    expression = str(item.get("expr") or "")
    properties: list[tuple[str, Value]] = []
    if _windowed(expression):
        properties.append(("window_expression", _spaced(expression)))
    else:
        properties.append(("expression", expression))
    properties += [
        ("using", _upper(_strings(item.get("using_relationships")))),
        (
            "non_additive_by",
            tuple(_yaml_sort_key(entry) for entry in _items(item.get("non_additive_dimensions"))),
        ),
        ("synonyms", _strings(item.get("synonyms"))),
        ("comment", str(item.get("description") or "")),
        ("access", "private" if str(item.get("access_modifier") or "") == "private_access" else "public"),
    ]
    return Member("metric", _display(qualified), tuple(properties))


def _yaml_relationship(item: Mapping[str, Any]) -> Member:
    """A relationship; an ASOF or range one compares its referenced column or bounds, not its columns."""
    columns = _items(item.get("relationship_columns"))
    asof = next((str(entry.get("right_column") or "") for entry in columns if entry.get("type") == "asof"), "")
    ranged = next((_entry(entry.get("right_range")) for entry in columns if entry.get("type") == "range"), None)
    properties: list[tuple[str, Value]] = [
        ("table", _display(str(item.get("left_table") or ""))),
        ("columns", _upper(tuple(str(entry.get("left_column") or "") for entry in columns))),
        ("ref_table", _display(str(item.get("right_table") or ""))),
    ]
    if not asof and ranged is None:
        properties.append(("ref_columns", _upper(tuple(str(entry.get("right_column") or "") for entry in columns))))
    bounds = f"{ranged.get('start_column') or ''} AND {ranged.get('end_column') or ''}" if ranged is not None else ""
    properties += [("asof_column", asof.upper()), ("range", bounds.upper())]
    return Member("relationship", _display(str(item.get("name") or "")), tuple(properties))


def _yaml_query(item: Mapping[str, Any]) -> Member:
    verified_at = item.get("verified_at")
    return Member(
        "verified_query",
        _display(str(item.get("name") or "")),
        (
            ("question", str(item.get("question") or "")),
            ("sql", str(item.get("sql") or "")),
            ("verified_at", "" if verified_at is None else str(verified_at)),
            ("verified_by", str(item.get("verified_by") or "")),
            ("onboarding_question", "true" if item.get("use_as_onboarding_question") is True else ""),
        ),
    )


def _yaml_sort_key(item: Mapping[str, Any]) -> str:
    column = f"{item.get('table')}.{item.get('dimension')}".upper()
    descending = str(item.get("sort_direction") or "").casefold() == "descending"
    nulls = str(item.get("null_order") or ("first" if descending else "last")).upper()
    return f"{column} {'DESC' if descending else 'ASC'} NULLS {nulls}"


def _variable(name: str, data_type: str, default: str, comment: str) -> Member:
    """A variable, its type by its family alone and its default unquoted, as both sides spell them alike."""
    return Member(
        "variable",
        _display(name),
        (
            ("data_type", data_type.split("(", 1)[0].strip().upper()),
            ("default", default.strip().strip("'").casefold()),
            ("comment", comment),
        ),
    )


def _staleness(text: str) -> Member:
    return Member("setting", "max_staleness", (("text", " ".join(text.split()).casefold()),))


def _windowed(expression: str) -> bool:
    """Report whether a metric expression ends in an `OVER (...)` outside any parentheses or quotes."""
    depth = 0
    quoted = False
    upper = expression.upper()
    for index, char in enumerate(upper):
        if char == "'":
            quoted = not quoted
        elif not quoted and char == "(":
            depth += 1
        elif not quoted and char == ")":
            depth -= 1
        elif not quoted and depth == 0 and upper.startswith("OVER", index):
            before = upper[index - 1] if index else " "
            after = upper[index + 4 : index + 5]
            if not (before.isalnum() or before == "_") and after in (" ", "("):
                return True
    return False


def _spaced(text: str) -> str:
    """Text with each run of whitespace one space, as a window's rendered and live forms compare."""
    return " ".join(text.split())


def _items(value: object) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in value if isinstance(item, Mapping)) if isinstance(value, list) else ()


def _entry(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def _live_member(kind: str | None, parent: str | None, name: str, values: dict[str, list[str]]) -> Member | None:
    """One member from its DESCRIBE rows; None for the view's own rows and the custom instructions."""
    if kind == "TABLE":
        parts = tuple(_first(values, f"BASE_TABLE_{part}NAME") for part in ("DATABASE_", "SCHEMA_", ""))
        base = QualifiedName(*(Identifier.shown(part) for part in parts)).sql if all(parts) else ""
        return Member(
            "table",
            _display(name),
            (
                ("base_table", base),
                ("primary_key", _json_list(_first(values, "PRIMARY_KEY"))),
                (
                    "unique_keys",
                    tuple(", ".join(key) for text in values.get("UNIQUE_KEY", ()) for key in _nested(text)),
                ),
                ("synonyms", _json_list(_first(values, "SYNONYMS"))),
                ("comment", _first(values, "COMMENT")),
            ),
        )
    if kind == "CONSTRAINT":
        return _constraint(
            name, _first(values, "CONSTRAINT_TYPE"), _first(values, "START_COLUMN"), _first(values, "END_COLUMN")
        )
    if kind == "RELATIONSHIP":
        return Member(
            "relationship",
            _display(name),
            (
                ("table", _display(_first(values, "TABLE"))),
                ("columns", _json_list(_first(values, "FOREIGN_KEY"))),
                ("ref_table", _display(_first(values, "REF_TABLE"))),
                ("ref_columns", _json_list(_first(values, "REF_KEY"))),
            ),
        )
    if kind in ("FACT", "DIMENSION"):
        labels = {label.casefold() for label in _json_list(_first(values, "LABELS"))}
        member_kind = "filter" if "filter" in labels else kind.lower()
        properties: list[tuple[str, Value]] = [
            ("expression", _first(values, "EXPRESSION")),
            ("synonyms", _json_list(_first(values, "SYNONYMS"))),
            ("comment", _first(values, "COMMENT")),
            ("sample_values", _json_list(_first(values, "SAMPLE_VALUES"))),
        ]
        if member_kind == "dimension":
            properties.append(("is_enum", "true" if _first(values, "IS_ENUM").casefold() == "true" else ""))
        return Member(member_kind, _display(f"{parent}.{name}"), tuple(properties))
    if kind in ("METRIC", "DERIVED_METRIC"):
        qualified = f"{parent}.{name}" if kind == "METRIC" and parent else name
        return Member(
            "metric",
            _display(qualified),
            (
                ("expression", _first(values, "EXPRESSION")),
                (
                    "non_additive_by",
                    tuple(_live_sort_key(item) for item in _json_objects(_first(values, "NON_ADDITIVE_DIMENSIONS"))),
                ),
                ("synonyms", _json_list(_first(values, "SYNONYMS"))),
                ("comment", _first(values, "COMMENT")),
                ("access", _first(values, "ACCESS_MODIFIER").casefold() or "public"),
            ),
        )
    if kind == "AI_VERIFIED_QUERY":
        return Member(
            "verified_query",
            _display(name),
            (
                ("question", _first(values, "QUESTION")),
                ("sql", _first(values, "SQL")),
                ("verified_at", _first(values, "VERIFIED_AT")),
                ("verified_by", _first(values, "VERIFIED_BY")),
                ("onboarding_question", "true" if _first(values, "ONBOARDING_QUESTION").casefold() == "true" else ""),
            ),
        )
    return None


def _table(table: Table) -> Member:
    return Member(
        "table",
        _display(table.logical_name),
        (
            ("base_table", QualifiedName.parse(table.fqn).sql),
            ("primary_key", _upper(table.primary_key)),
            ("unique_keys", tuple(", ".join(_upper(key)) for key in table.unique_keys)),
            ("synonyms", table.synonyms),
            ("comment", table.comment or ""),
        ),
    )


def _constraint(name: str, constraint_type: str, start: str, end: str) -> Member:
    return Member(
        "constraint",
        _display(name),
        (("type", constraint_type.upper()), ("start_column", start.upper()), ("end_column", end.upper())),
    )


def _column(column: Column) -> Member:
    kind = _COLUMN_KINDS[column.kind]
    properties: list[tuple[str, Value]] = [
        ("expression", column.expr.text),
        ("synonyms", column.synonyms),
        ("comment", column.comment or ""),
        ("sample_values", column.sample_values),
    ]
    if kind == "dimension":
        properties.append(("is_enum", "true" if column.is_enum else ""))
    return Member(kind, _display(column.qualified_name), tuple(properties))


def _metric(metric: Metric) -> Member:
    properties: list[tuple[str, Value]] = []
    if metric.window is None:
        properties.append(("expression", metric.expr.text))
    else:
        # DESCRIBE has not been seen to report a window metric's expression; the YAML read does,
        # with its OVER clause, which only this name compares.
        properties.append(("window_expression", _spaced(f"{metric.expr.text} {render_window(metric.window)}")))
    properties += [
        ("using", _upper(metric.using_relationships)),
        ("non_additive_by", tuple(_sort_key(key) for key in metric.non_additive_by)),
        ("synonyms", metric.synonyms),
        ("comment", metric.comment or ""),
        ("access", "private" if metric.access_modifier == "private_access" else "public"),
    ]
    return Member("metric", _display(metric.qualified_name), tuple(properties))


def _sort_key(key: SortKey) -> str:
    """A sort key in full: unstated order is ascending, and unstated nulls take that order's default."""
    descending = bool(key.descending)
    nulls_first = key.nulls_first if key.nulls_first is not None else descending
    return f"{key.expr.text.upper()} {'DESC' if descending else 'ASC'} NULLS {'FIRST' if nulls_first else 'LAST'}"


def _live_sort_key(item: dict[str, object]) -> str:
    column = f"{item.get('table')}.{item.get('dimension')}".upper()
    descending = str(item.get("order") or "").upper() == "DESCENDING"
    nulls = str(item.get("null_order") or ("FIRST" if descending else "LAST")).upper()
    return f"{column} {'DESC' if descending else 'ASC'} NULLS {nulls}"


def _first(values: dict[str, list[str]], prop: str) -> str:
    found = values.get(prop)
    return found[0] if found else ""


def _json_list(text: str) -> tuple[str, ...]:
    """A JSON list of strings DESCRIBE returns, such as `["aov","order size"]`; empty when none."""
    parsed = _parse(text)
    return tuple(str(item) for item in parsed) if isinstance(parsed, list) else ()


def _nested(text: str) -> tuple[tuple[str, ...], ...]:
    """A JSON list of lists, as DESCRIBE returns a table's unique keys: `[["A","B"]]`."""
    parsed = _parse(text)
    if not isinstance(parsed, list):
        return ()
    return tuple(tuple(str(part) for part in key) if isinstance(key, list) else (str(key),) for key in parsed)


def _json_objects(text: str) -> tuple[dict[str, object], ...]:
    parsed = _parse(text)
    return tuple(item for item in parsed if isinstance(item, dict)) if isinstance(parsed, list) else ()


def _parse(text: str) -> object:
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def _upper(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(value.upper() for value in values)


def _display(name: str) -> str:
    """A member's name as a person reads it: lowercased unless it holds a lowercase letter already."""
    return name if any(char.islower() for char in name) else name.lower()


def _sorted(members: list[Member]) -> tuple[Member, ...]:
    return tuple(sorted(members, key=lambda member: (_ORDER.index(member.kind), member.name.casefold())))
