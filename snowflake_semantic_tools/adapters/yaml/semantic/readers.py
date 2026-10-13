"""Read metrics, filters, custom instructions and verified queries into their member records."""

from __future__ import annotations

import posixpath
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.fields import sibling_within
from snowflake_semantic_tools.adapters.yaml.semantic.defs import _non_additive, _window
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _as_str_tuple, _referenced_tables
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import member_reference
from snowflake_semantic_tools.domain.validate.semantic.deprecated import honoured
from snowflake_semantic_tools.domain.validate.semantic.nodes import list_of, load_nodes, member_root, node_origin


def _relationship_name(text: str) -> str:
    """Read a `using_relationships` entry: a bare relationship name, or one `relationship()` call."""
    return member_reference(text, "relationship") or text


def load_metrics(documents: RawDocuments, project_dir: Path, semantic_models_dir: str) -> tuple[MetricDef, ...]:
    """Read every document's `snowflake_metrics:` entries into records, in document order.

    A file in any folder is read: the `metrics/` directory the arguments name is not consulted.
    An entry that is not a mapping, or has no `name` or no `expr`, is skipped here, for the checks
    to report. A metric's tables are the models its expression names with `ref()`; a window
    metric that names none takes those of the metrics it aggregates, or, failing those, the
    models its `window:` names. It is derived when its expression combines other metrics with
    `metric()`, names no column, and it has no `window:`.

    Raises:
        ProjectError: An entry's `synonyms` is a mapping, which cannot be read as text.
    """
    out: list[MetricDef] = []
    for document, index, node in load_nodes(documents, member_root("metric")):
        if not node.get("name") or not node.get("expr"):
            continue
        expression = str(node["expr"])
        try:
            template_calls = scan_template_calls(expression)
        except TemplateSyntaxError:
            template_calls = ()
        window = _window(node.get("window"))
        tables, poisoned = _referenced_tables(expression)
        _, window_poisoned = _referenced_tables(*(text for _, text in (window.references() if window else ())))
        out.append(
            MetricDef(
                name=str(node["name"]),
                expr=expression,
                description=" ".join(str(node.get("description") or "").splitlines()).strip() or None,
                synonyms=_as_str_tuple(node.get("synonyms")),
                tables=tables,
                derived=window is None and not tables and any(call.function == "metric" for call in template_calls),
                using_relationships=tuple(
                    _relationship_name(str(value)).upper() for value in node.get("using_relationships") or []
                ),
                relationship_refs=tuple(
                    name.upper()
                    for value in node.get("using_relationships") or []
                    if (name := member_reference(str(value), "relationship")) is not None
                ),
                non_additive=tuple(
                    _non_additive(value)
                    for value in list_of(node.get("non_additive_dimensions"))
                    if isinstance(value, dict)
                    and isinstance(value.get("dimension"), str)
                    and value["dimension"].strip()
                ),
                access_modifier=str(honoured(node, "access_modifier", "metric") or "public_access"),
                origin=node_origin(document, member_root("metric"), index),
                template_calls=template_calls,
                poisoned=poisoned or window_poisoned,
                window=window,
            )
        )
    return _window_tables(tuple(out))


def _window_tables(metrics: tuple[MetricDef, ...]) -> tuple[MetricDef, ...]:
    """Give each window metric that names no table the tables of the metrics it aggregates.

    `SUM({{ metric('customer_count') }})` over a window reads `customer_count`'s table. The
    pass repeats until nothing changes, so a window over such a metric resolves too, and a
    cycle, which SST-REF005 reports, ends it. A window metric that still has none takes the
    models its `window:` names.
    """
    by_name = {metric.name.casefold(): metric for metric in metrics}
    changed = True
    while changed:
        changed = False
        for name, metric in by_name.items():
            if metric.window is None or metric.tables:
                continue
            inherited = tuple(
                dict.fromkeys(
                    table
                    for referenced in metric.referenced_metrics
                    if referenced in by_name
                    for table in by_name[referenced].tables
                )
            )
            if inherited:
                by_name[name] = replace(metric, tables=inherited)
                changed = True
    for name, metric in by_name.items():
        if metric.window is not None and not metric.tables:
            by_name[name] = replace(
                metric, tables=_referenced_tables(*(text for _, text in metric.window.references()))[0]
            )
    return tuple(
        replace(metric, tables=by_name[metric.name.casefold()].tables) if metric.window else metric
        for metric in metrics
    )


def load_filters(documents: RawDocuments, project_dir: Path, semantic_models_dir: str) -> tuple[FilterDef, ...]:
    """Read every document's `snowflake_filters:` entries into records, in document order.

    A file in any folder is read: the `filters/` directory the arguments name is not consulted.
    An entry that is not a mapping, or has no `name` or no `expr`, is skipped here. `labels`
    compare casefolded, and a `labels:` value that is not a list holds none. A filter's tables
    are the models its expression names with `ref()`.
    """
    out: list[FilterDef] = []
    for document, index, node in load_nodes(documents, member_root("filter")):
        if not node.get("name") or not node.get("expr"):
            continue
        labels = node.get("labels")
        labels = {str(label).casefold() for label in labels} if isinstance(labels, list) else set()
        expression = str(node["expr"])
        try:
            template_calls = scan_template_calls(expression)
        except TemplateSyntaxError:
            template_calls = ()
        tables, poisoned = _referenced_tables(expression)
        out.append(
            FilterDef(
                name=str(node["name"]),
                expr=expression,
                description=str(node.get("description") or "").strip() or None,
                tables=tables,
                entity_level="filter" in labels,
                origin=node_origin(document, member_root("filter"), index),
                template_calls=template_calls,
                poisoned=poisoned,
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
    out: dict[str, InstructionDef] = {}
    for document, index, node in load_nodes(documents, member_root("custom_instruction")):
        if not node.get("name"):
            continue
        name = str(node["name"])
        out[name.casefold()] = InstructionDef(
            name=name,
            ai_sql_generation=str(honoured(node, "ai_sql_generation", "custom_instruction") or "").strip() or None,
            ai_question_categorization=str(
                honoured(node, "ai_question_categorization", "custom_instruction") or ""
            ).strip()
            or None,
            origin=node_origin(document, member_root("custom_instruction"), index),
            renamed="sql_generation" in node or "question_categorization" in node,
        )
    return out


def _strip_sql_header(sql: str) -> str:
    lines = sql.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    return "\n".join(lines).rstrip()


def _calls_file(text: str) -> bool:
    """Whether `text` holds a `file()` call; a template that does not parse holds none."""
    try:
        return any(call.function == "file" for call in scan_template_calls(text))
    except TemplateSyntaxError:
        return False


def _verified_at(value: object, *, name: str) -> int | None:
    """Read `verified_at` as epoch seconds: an integer, or an ISO-8601 string, in UTC unless it says.

    Raises:
        ProjectError: The value is neither (SST-PRS017).

    Diagnostics:
        SST-PRS017: the value is not an integer epoch or an ISO-8601 string; raised.
    """
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError:
            parsed = None
        if parsed is not None:
            return int((parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).timestamp())
    subject = artifact_key("verified_query", name)
    diagnostic = D("SST-PRS017", artifact=subject, field="verified_at", found=str(value), subject=subject)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def load_verified_queries(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> tuple[VerifiedQueryDef, ...]:
    """Read every document's `snowflake_verified_queries:` entries into records, in document order.

    A file in any folder is read. The SQL is `sql:`, or the file a `{{ file('<path>') }}` call
    that is all of it names, relative to the entry's own file. Its tables are the models the
    SQL, the file's when it names one, names with `ref()`. An entry is skipped here when it is
    not a mapping, has no `name`, `question` or `sql`, calls `file()` in a `sql:` that call is
    not all of, or names a file that is, or is reached through, a symbolic link, lies outside
    the project, cannot be read, is not UTF-8, or holds only whitespace; the shape checks
    report those.

    Raises:
        ProjectError: An entry's `verified_at` is neither an integer nor an ISO-8601 string; an
            unquoted date, which YAML reads as a date, is refused too (SST-PRS017).
    """
    out: list[VerifiedQueryDef] = []
    for document, index, node in load_nodes(documents, member_root("verified_query")):
        if not node.get("name") or not node.get("question") or node.get("sql") is None:
            continue
        sql = str(node["sql"])
        sql_file = None
        requested = member_reference(sql.strip(), "file")
        if requested is None and _calls_file(sql):
            # A file() that is not all of `sql:` is SST-REF003's to report.
            continue
        if requested is not None:
            raw = documents.by_path[document.path]
            sql_path, refusal = sibling_within(raw.abs_path, document.path, requested)
            if refusal is not None:
                continue
            try:
                sql = sql_path.read_text(encoding="utf-8").rstrip("\n")
            except (OSError, UnicodeDecodeError):
                continue
            if not sql.strip():
                continue
            sql_file = posixpath.normpath(posixpath.join(posixpath.dirname(document.path), requested))
        sql = _strip_sql_header(sql)
        name = str(node["name"])
        try:
            template_calls = scan_template_calls(sql)
        except TemplateSyntaxError:
            template_calls = ()
        tables, poisoned = _referenced_tables(sql)
        out.append(
            VerifiedQueryDef(
                name=name,
                question=str(node["question"]),
                sql=sql,
                tables=tables,
                verified_at=_verified_at(node.get("verified_at"), name=name),
                verified_by=str(node.get("verified_by") or "").strip() or None,
                onboarding_question=(
                    bool(node["use_as_onboarding_question"]) if "use_as_onboarding_question" in node else None
                ),
                origin=node_origin(document, member_root("verified_query"), index),
                template_calls=template_calls,
                poisoned=poisoned,
                sql_file=sql_file,
            )
        )
    return tuple(out)
