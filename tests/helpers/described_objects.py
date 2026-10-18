"""What DESCRIBE returns for a published semantic view, search service or routine, made from its model.

A fake Snowflake answers a describe with these, so a plan against it can compare an update member
by member. The shapes are those a live account returned for a published view: JSON lists for keys,
synonyms and sample values, `[[...]]` unique keys, `["filter"]` labels, `TRUE` for an onboarding
question, and no row for what a member does not set. A window metric, an ASOF or range
relationship, and a variable are described as plain ones: what plan compares of them is only what
DESCRIBE has been seen to report.
"""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle.marker import marked_comment
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind, SemanticView
from snowflake_semantic_tools.domain.model.tool import ToolMember
from snowflake_semantic_tools.domain.ports.snowflake.catalog import DescribedMember


def described_view(view: SemanticView) -> tuple[DescribedMember, ...]:
    """Return what DESCRIBE SEMANTIC VIEW shows of `view` once published, the members its scope admits."""
    rows: list[DescribedMember] = []

    def add(kind: str | None, name: str | None, prop: str, value: str | None, parent: str | None = None) -> None:
        if value:
            rows.append(DescribedMember(kind, name, parent, prop, value))

    for table in view.tables:
        base = QualifiedName.parse(table.fqn)
        add("TABLE", table.logical_name, "BASE_TABLE_DATABASE_NAME", base.database.value)
        add("TABLE", table.logical_name, "BASE_TABLE_SCHEMA_NAME", base.schema.value)
        add("TABLE", table.logical_name, "BASE_TABLE_NAME", base.name.value)
        add("TABLE", table.logical_name, "PRIMARY_KEY", _json(table.primary_key))
        add(
            "TABLE",
            table.logical_name,
            "UNIQUE_KEY",
            json.dumps([list(key) for key in table.unique_keys]) if table.unique_keys else "",
        )
        add("TABLE", table.logical_name, "SYNONYMS", _json(table.synonyms))
        add("TABLE", table.logical_name, "COMMENT", table.comment)
        if table.distinct_range is not None:
            name = f"{table.logical_name}_DISTINCT_RANGE"
            add("CONSTRAINT", name, "CONSTRAINT_TYPE", "DISTINCT_RANGE")
            add("CONSTRAINT", name, "START_COLUMN", table.distinct_range[0])
            add("CONSTRAINT", name, "END_COLUMN", table.distinct_range[1])
    scope = view.scope
    for rel in view.relationships:
        if scope.admits_relationship(rel.name):
            add("RELATIONSHIP", rel.name, "TABLE", rel.from_table)
            add("RELATIONSHIP", rel.name, "REF_TABLE", rel.to_table)
            add("RELATIONSHIP", rel.name, "FOREIGN_KEY", _json(rel.from_columns))
            add("RELATIONSHIP", rel.name, "REF_KEY", _json(rel.to_columns))
    for column in view.columns:
        if not scope.admits_column(column):
            continue
        kind = "FACT" if column.kind is ColumnKind.FACT else "DIMENSION"
        name = column.qualified_name.split(".", 1)[1]
        add(kind, name, "EXPRESSION", column.expr.text, column.table)
        add(kind, name, "LABELS", '["filter"]' if column.kind is ColumnKind.FILTER else "", column.table)
        add(kind, name, "SYNONYMS", _json(column.synonyms), column.table)
        add(kind, name, "COMMENT", column.comment, column.table)
        add(kind, name, "SAMPLE_VALUES", _json(column.sample_values), column.table)
        add(kind, name, "IS_ENUM", "true" if column.is_enum else "", column.table)
    for metric in view.metrics:
        if not scope.admits_metric(metric.name):
            continue
        kind = "METRIC" if metric.table else "DERIVED_METRIC"
        add(kind, metric.name, "EXPRESSION", metric.expr.text, metric.table)
        keys = [
            {
                "table": key.expr.text.split(".")[0],
                "dimension": key.expr.text.split(".")[-1],
                "order": "DESCENDING" if key.descending else "ASCENDING",
                "null_order": "FIRST"
                if (key.nulls_first if key.nulls_first is not None else key.descending)
                else "LAST",
            }
            for key in metric.non_additive_by
        ]
        add(kind, metric.name, "NON_ADDITIVE_DIMENSIONS", json.dumps(keys) if keys else "", metric.table)
        add(kind, metric.name, "SYNONYMS", _json(metric.synonyms), metric.table)
        add(kind, metric.name, "COMMENT", metric.comment, metric.table)
        access = "PRIVATE" if metric.access_modifier == "private_access" else "PUBLIC"
        add(kind, metric.name, "ACCESS_MODIFIER", access, metric.table)
    for query in view.verified_queries:
        add("AI_VERIFIED_QUERY", query.name, "QUESTION", query.question)
        add("AI_VERIFIED_QUERY", query.name, "SQL", query.sql.text)
        add(
            "AI_VERIFIED_QUERY",
            query.name,
            "VERIFIED_AT",
            None if query.verified_at is None else str(query.verified_at),
        )
        add("AI_VERIFIED_QUERY", query.name, "VERIFIED_BY", query.verified_by)
        add("AI_VERIFIED_QUERY", query.name, "ONBOARDING_QUESTION", "TRUE" if query.onboarding_question else "")
    add("CUSTOM_INSTRUCTION", None, "AI_SQL_GENERATION", view.ai_sql_generation)
    add("CUSTOM_INSTRUCTION", None, "AI_QUESTION_CATEGORIZATION", view.ai_question_categorization)
    add(None, None, "COMMENT", marked_comment(view.comment, view.ownership_marker or ""))
    return tuple(rows)


def _json(values: tuple[str, ...]) -> str:
    return json.dumps(list(values)) if values else ""


def described_search(member: ToolMember, query: str, comment: str | None = None) -> dict[str, str]:
    """Return the one row DESCRIBE CORTEX SEARCH SERVICE shows of a published search service member."""
    columns = dict.fromkeys(
        (*(column.name for column in member.columns), *member.attribute_columns, member.search_column)
    )
    return {
        "search_column": (member.search_column or "").upper(),
        "attribute_columns": ",".join(name.upper() for name in member.attribute_columns),
        "columns": ",".join(name.upper() for name in columns if name),
        "warehouse": (member.warehouse or "").upper(),
        "target_lag": member.target_lag or "",
        "embedding_model": member.embedding_model or "snowflake-arctic-embed-m-v1.5",
        "refresh_mode": (member.refresh_mode or "INCREMENTAL").upper(),
        "definition": query,
        "comment": comment if comment is not None else member.description or "",
    }


def described_routine(member: ToolMember) -> dict[str, str]:
    """Return the rows DESCRIBE PROCEDURE or FUNCTION shows of a published routine member, by property."""
    signature = ", ".join(f"{parameter.name.upper()} {parameter.type}" for parameter in member.signature)
    rows = {
        "signature": f"({signature})",
        "returns": member.returns or "",
        "language": (member.language or "").upper(),
        "body": f"\n{(member.body or '').rstrip()}\n",
    }
    if member.type == "procedure":
        rows["execute as"] = (member.execute_as or "caller").upper()
    if (member.language or "").casefold() in ("java", "python", "scala"):
        rows |= {
            "runtime_version": member.runtime_version or "",
            "handler": member.handler or "",
            "packages": str(list(member.packages)),
            "imports": str(list(member.imports)),
        }
    return rows
