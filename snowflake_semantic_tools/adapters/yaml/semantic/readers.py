"""Read metrics, filters, custom instructions and verified queries into their member records."""

from __future__ import annotations

from datetime import UTC
from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.defs import (
    FilterDef,
    InstructionDef,
    MetricDef,
    VerifiedQueryDef,
    _non_additive,
    _window,
)
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import (
    _as_str_tuple,
    _list_of,
    _load_nodes,
    _member_root,
    _node_origin,
    _safe_table_refs,
    _table_refs_poisoned,
)
from snowflake_semantic_tools.domain.model.reference import TemplateSyntaxError, scan_template_calls


def load_metrics(documents: RawDocuments, project_dir: Path, semantic_models_dir: str) -> tuple[MetricDef, ...]:
    """Read every document's `snowflake_metrics:` entries into records, in document order.

    A file in any folder is read: the `metrics/` directory the arguments name is not consulted.
    An entry that is not a mapping, or has no `name` or no `expr`, is skipped here, for the checks
    to report.

    Raises:
        ProjectError: An entry's `synonyms` is a mapping, which cannot be read as text.
    """
    metrics_dir = project_dir / semantic_models_dir / "metrics"
    out: list[MetricDef] = []
    for document, index, node in _load_nodes(documents, metrics_dir, _member_root("metric")):
        if not node.get("name") or not node.get("expr"):
            continue
        expression = str(node["expr"])
        try:
            template_calls = scan_template_calls(expression)
        except TemplateSyntaxError:
            template_calls = ()
        out.append(
            MetricDef(
                name=str(node["name"]),
                expr=expression,
                description=" ".join(str(node.get("description") or "").splitlines()).strip() or None,
                synonyms=_as_str_tuple(node.get("synonyms")),
                tables=_safe_table_refs(node.get("tables")),
                derived=bool(node.get("derived", False)),
                using_relationships=tuple(str(value).upper() for value in node.get("using_relationships") or []),
                non_additive=tuple(
                    _non_additive(value)
                    for value in _list_of(node.get("non_additive_dimensions"))
                    if isinstance(value, dict)
                    and isinstance(value.get("dimension"), str)
                    and value["dimension"].strip()
                ),
                access_modifier=str(node.get("access_modifier") or "public_access"),
                has_tables_key="tables" in node,
                origin=_node_origin(document, _member_root("metric"), index),
                template_calls=template_calls,
                poisoned=_table_refs_poisoned(node.get("tables")),
                window=_window(node.get("window")),
            )
        )
    return tuple(out)


def load_filters(documents: RawDocuments, project_dir: Path, semantic_models_dir: str) -> tuple[FilterDef, ...]:
    """Read every document's `snowflake_filters:` entries into records, in document order.

    A file in any folder is read: the `filters/` directory the arguments name is not consulted.
    An entry that is not a mapping, or has no `name` or no `expr`, is skipped here. `labels`
    compare casefolded, and a `labels:` value that is not a list holds none.
    """
    out: list[FilterDef] = []
    root = project_dir / semantic_models_dir / "filters"
    for document, index, node in _load_nodes(documents, root, _member_root("filter")):
        if not node.get("name") or not node.get("expr"):
            continue
        labels = node.get("labels")
        labels = {str(label).casefold() for label in labels} if isinstance(labels, list) else set()
        expression = str(node["expr"])
        try:
            template_calls = scan_template_calls(expression)
        except TemplateSyntaxError:
            template_calls = ()
        out.append(
            FilterDef(
                name=str(node["name"]),
                expr=expression,
                description=str(node.get("description") or "").strip() or None,
                tables=_safe_table_refs(node.get("tables")),
                entity_level="filter" in labels,
                origin=_node_origin(document, _member_root("filter"), index),
                template_calls=template_calls,
                poisoned=_table_refs_poisoned(node.get("tables")),
                labeled="labels" in node,
            )
        )
    return tuple(out)


def load_instructions(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> dict[str, InstructionDef]:
    """Read every document's `snowflake_custom_instructions:` entries into records, by casefolded name.

    A file in any folder is read, and an entry that is not a mapping or has no `name` is skipped.
    Of two entries whose names casefold alike the later is kept; the name checks report the
    repeat (SST-PRS106).
    """
    root = project_dir / semantic_models_dir / "custom_instructions"
    out: dict[str, InstructionDef] = {}
    for document, index, node in _load_nodes(documents, root, _member_root("custom_instruction")):
        if not node.get("name"):
            continue
        name = str(node["name"])
        out[name.casefold()] = InstructionDef(
            name=name,
            ai_sql_generation=str(node.get("ai_sql_generation") or "").strip() or None,
            ai_question_categorization=str(node.get("ai_question_categorization") or "").strip() or None,
            origin=_node_origin(document, _member_root("custom_instruction"), index),
        )
    return out


def _strip_sql_header(sql: str) -> str:
    lines = sql.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    return "\n".join(lines).rstrip()


def _verified_at(value: object, *, path: Path, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            from datetime import datetime

            return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
        except ValueError as exc:
            raise ProjectError(f"{path}: verified query {name} has invalid verified_at {value!r}") from exc
    raise ProjectError(f"{path}: verified query {name} has unsupported verified_at {value!r}")


def load_verified_queries(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> tuple[VerifiedQueryDef, ...]:
    """Read every document's `snowflake_verified_queries:` entries into records, in document order.

    A file in any folder is read. The SQL is `sql:`, or the `sql_file:` it names relative to the
    entry's own file. An entry is skipped here when it is not a mapping, has no `name` or no
    `question`, declares both or neither of `sql` and `sql_file`, or names a file that cannot be
    read, is not UTF-8, or holds only whitespace; the shape checks report those files.

    Raises:
        ProjectError: An entry's `verified_at` is neither an integer nor a `YYYY-MM-DD` string; an
            unquoted date, which YAML reads as a date, is refused too.
    """
    root = project_dir / semantic_models_dir / "verified_queries"
    out: list[VerifiedQueryDef] = []
    for document, index, node in _load_nodes(documents, root, _member_root("verified_query")):
        if not node.get("name") or not node.get("question"):
            continue
        has_sql = node.get("sql") is not None
        has_sql_file = node.get("sql_file") is not None
        if has_sql == has_sql_file:
            continue
        sql = str(node.get("sql") or "")
        if not sql and node.get("sql_file"):
            sql_path = document.abs_path.parent / str(node["sql_file"])
            try:
                sql = sql_path.read_text(encoding="utf-8").rstrip("\n")
            except (OSError, UnicodeDecodeError):
                continue
            if not sql.strip():
                continue
        sql = _strip_sql_header(sql)
        name = str(node["name"])
        try:
            template_calls = scan_template_calls(sql)
        except TemplateSyntaxError:
            template_calls = ()
        out.append(
            VerifiedQueryDef(
                name=name,
                question=str(node["question"]),
                sql=sql,
                tables=_safe_table_refs(node.get("tables")),
                verified_at=_verified_at(node.get("verified_at"), path=document.abs_path, name=name),
                verified_by=str(node.get("verified_by") or "").strip() or None,
                onboarding_question=(
                    bool(node["use_as_onboarding_question"]) if "use_as_onboarding_question" in node else None
                ),
                origin=_node_origin(document, _member_root("verified_query"), index),
                template_calls=template_calls,
                poisoned=_table_refs_poisoned(node.get("tables")),
            )
        )
    return tuple(out)
