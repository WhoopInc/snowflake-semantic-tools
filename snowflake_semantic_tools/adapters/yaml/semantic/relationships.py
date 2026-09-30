"""Read relationships, and check them against each other and the views that hold their tables."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ....domain.model.artifact_key import artifact_key
from ....domain.model.dbt import DbtModel
from ....domain.model.diagnostic import D, Diagnostic, Origin
from ....domain.model.reference import TemplateSyntaxError, scan_template_calls
from ....domain.model.semantic_view import Relationship
from ..documents import RawDocuments
from .defs import MetricDef
from .nodes import _load_nodes, _member_root, _node_origin


def _relationship_diagnostics(
    relationships: tuple[Relationship, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
    origins: Mapping[str, Origin] | None = None,
    models: Mapping[str, DbtModel] | None = None,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for relationship in relationships:
        origin = (origins or {}).get(relationship.name.casefold())
        endpoints = {
            relationship.from_table.casefold(),
            relationship.to_table.casefold(),
        }
        subject = artifact_key("relationship", relationship.name.casefold())
        if not any(endpoints.issubset(tables) for _, tables in view_table_sets):
            closest_name, closest_tables = max(
                view_table_sets,
                key=lambda item: len(endpoints.intersection(item[1])),
                default=("semantic_view:<none>", frozenset()),
            )
            missing = sorted(endpoints - closest_tables)[0]
            diagnostics.append(
                D(
                    "SST-VAL203",
                    relationship=relationship.name.casefold(),
                    name=missing,
                    artifact=closest_name,
                    subject=subject,
                    origin=origin,
                )
            )
            diagnostics.append(
                D(
                    "SST-MEM005",
                    member=subject,
                    type="semantic_view",
                    subject=subject,
                    caused_by="SST-VAL203",
                    origin=origin,
                )
            )
            continue
        target = (models or {}).get(relationship.to_table.casefold())
        if target is None:
            continue
        if relationship.asof_index is not None or relationship.range_bounds is not None:
            continue
        join_columns = {column.casefold() for column in relationship.to_columns}
        keys = tuple(key for key in (target.primary_key, *target.unique_keys) if key)
        if not keys:
            # Snowflake refuses a REFERENCES target that declares no key at all.
            diagnostics.append(
                D(
                    "SST-VAL311",
                    artifact=subject,
                    name=target.name,
                    subject=subject,
                    origin=origin,
                )
            )
        elif not any({column.casefold() for column in key}.issubset(join_columns) for key in keys):
            diagnostics.append(
                D(
                    "SST-VAL210",
                    relationship=relationship.name.casefold(),
                    name=target.name,
                    value=", ".join(relationship.to_columns),
                    subject=subject,
                    origin=origin,
                )
            )
    return tuple(diagnostics)


def _relationship_cycle_diagnostics(
    relationships: tuple[Relationship, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
) -> tuple[Diagnostic, ...]:
    """Snowflake refuses a view whose relationships form a cycle, a self-reference included."""
    diagnostics: list[Diagnostic] = []
    for artifact, tables in view_table_sets:
        edges: dict[str, set[str]] = {}
        for relationship in relationships:
            left, right = relationship.from_table.casefold(), relationship.to_table.casefold()
            if left in tables and right in tables:
                edges.setdefault(left, set()).add(right)
        cycle = _first_cycle({node: tuple(sorted(targets)) for node, targets in edges.items()})
        if cycle is not None:
            diagnostics.append(D("SST-VAL215", artifact=artifact, cycle=" -> ".join(cycle), subject=artifact))
    return tuple(diagnostics)


def _first_cycle(edges: Mapping[str, tuple[str, ...]]) -> tuple[str, ...] | None:
    visiting: list[str] = []
    done: set[str] = set()

    def visit(node: str) -> tuple[str, ...] | None:
        visiting.append(node)
        for target in edges.get(node, ()):
            if target in visiting:
                return (*visiting[visiting.index(target) :], target)
            if target not in done:
                found = visit(target)
                if found is not None:
                    return found
        visiting.pop()
        done.add(node)
        return None

    for node in sorted(edges):
        if node not in done:
            found = visit(node)
            if found is not None:
                return found
    return None


def _relationship_parse_diagnostics(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
) -> tuple[Diagnostic, ...]:
    root = project_dir / semantic_models_dir / "relationships"
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, root, _member_root("relationship")):
        if not node.get("name"):
            continue
        conditions = node.get("relationship_conditions")
        # The 0.3 `relationship_columns` spelling is reported as renamed instead.
        if (not isinstance(conditions, list) or not conditions) and "relationship_columns" not in node:
            name = str(node["name"])
            diagnostics.append(
                D(
                    "SST-VAL201",
                    relationship=name,
                    subject=artifact_key("relationship", name),
                    origin=_node_origin(document, _member_root("relationship"), index),
                )
            )
    return tuple(diagnostics)


def _multipath_diagnostics(
    relationships: tuple[Relationship, ...],
    metrics: tuple[MetricDef, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
) -> tuple[Diagnostic, ...]:
    pair_counts: dict[tuple[str, str], int] = {}
    for relationship in relationships:
        first, second = sorted((relationship.from_table.casefold(), relationship.to_table.casefold()))
        pair = (first, second)
        pair_counts[pair] = pair_counts.get(pair, 0) + 1
    diagnostics: list[Diagnostic] = []
    for pair, count in sorted(pair_counts.items()):
        if count < 2:
            continue
        a, b = pair
        diagnostics.extend(
            D("SST-VAL209", artifact=artifact, count=count, a=a, b=b)
            for artifact, tables in view_table_sets
            if {a, b}.issubset(tables)
        )
        for metric in metrics:
            if not metric.tables or metric.using_relationships:
                continue
            owner = metric.tables[0]
            if owner == a:
                diagnostics.append(
                    D(
                        "SST-VAL116",
                        metric=metric.name,
                        count=count,
                        name=b,
                        subject=artifact_key("metric", metric.name),
                    )
                )
                break
            elif owner == b:
                diagnostics.append(
                    D(
                        "SST-VAL116",
                        metric=metric.name,
                        count=count,
                        name=a,
                        subject=artifact_key("metric", metric.name),
                    )
                )
                break
    return tuple(diagnostics)


def _uses_legacy_globals(value: object) -> bool:
    try:
        calls = scan_template_calls(str(value))
    except TemplateSyntaxError:
        return False
    return any(call.function in ("table", "column") for call in calls)


_ENDPOINT_REF = re.compile(r"^\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}$")


def _endpoint_diagnostics(node: Mapping[str, Any], subject: str, origin: Origin) -> tuple[Diagnostic, ...]:
    """A 0.3 `{{ ref('x') }}` endpoint: `left_table` and `right_table` take the bare model name."""
    return tuple(
        D("SST-REF045", origin=origin, subject=subject, artifact=subject, field=field, found=str(node[field]).strip())
        for field in ("left_table", "right_table")
        if _ENDPOINT_REF.fullmatch(str(node.get(field) or "").strip())
    )


def load_relationships(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
    models: Mapping[str, DbtModel] | None = None,
) -> tuple[tuple[tuple[Relationship, Origin], ...], tuple[Diagnostic, ...]]:
    """Every well-formed relationship, and one diagnostic for each that is not.

    A malformed relationship is reported and left out rather than stopping the
    load, so one bad entry cannot hide every other diagnostic in the project.
    """
    root = project_dir / semantic_models_dir / "relationships"
    out: list[tuple[Relationship, Origin]] = []
    diagnostics: list[Diagnostic] = []
    equality = re.compile(
        r"^\s*\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*=\s*"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*$"
    )
    asof = re.compile(
        r"^\s*\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*>=\s*"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*$"
    )
    range_condition = re.compile(
        r"^\s*\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s+BETWEEN\s+"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s+AND\s+"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*$",
        re.IGNORECASE,
    )
    for document, index, node in _load_nodes(documents, root, _member_root("relationship")):
        if not node.get("name"):
            continue
        raw_conditions = node.get("relationship_conditions")
        if not isinstance(raw_conditions, list) or not raw_conditions:
            continue
        if any(_uses_legacy_globals(condition) for condition in raw_conditions):
            # SST-REF034/SST-REF035 report the legacy globals; parsing further
            # would only bury that diagnostic under a condition-shape error.
            continue
        pairs: list[tuple[str, str]] = []
        asof_index: int | None = None
        range_bounds: tuple[str, str] | None = None
        origin = _node_origin(document, _member_root("relationship"), index)
        subject = artifact_key("relationship", node["name"])
        endpoint_problems = _endpoint_diagnostics(node, subject, origin)
        if endpoint_problems:
            diagnostics.extend(endpoint_problems)
            continue
        left_endpoint = str(node.get("left_table")).strip()
        right_endpoint = str(node.get("right_table")).strip()
        problem: Diagnostic | None = None
        for condition in raw_conditions:
            match = equality.fullmatch(str(condition))
            if match is not None:
                left_table, left_column, right_table, right_column = match.groups()
            else:
                match = asof.fullmatch(str(condition))
                if match is not None:
                    left_table, left_column, right_table, right_column = match.groups()
                    asof_index = len(pairs)
                else:
                    range_match = range_condition.fullmatch(str(condition))
                    if range_match is None or range_match.group(3).casefold() != range_match.group(5).casefold():
                        problem = D("SST-PRS110", origin=origin, subject=subject, artifact=subject, value=condition)
                        break
                    (
                        left_table,
                        left_column,
                        right_table,
                        range_start,
                        _range_table,
                        range_end,
                    ) = range_match.groups()
                    right_column = range_start
                    range_bounds = (range_start.upper(), range_end.upper())
            endpoints = (left_endpoint, right_endpoint)
            mismatch = next(
                (
                    (table, column, endpoint)
                    for table, column, endpoint in (
                        (left_table, left_column, endpoints[0]),
                        (right_table, right_column, endpoints[1]),
                    )
                    if table.casefold() != endpoint.casefold()
                ),
                None,
            )
            if mismatch is not None:
                table, column, endpoint = mismatch
                problem = D(
                    "SST-VAL204",
                    origin=origin,
                    subject=subject,
                    relationship=node["name"],
                    column=f"{table}.{column}",
                    name=endpoint,
                )
                break
            pairs.append((left_column.upper(), right_column.upper()))
        if problem is None and models is not None:
            for table_name, column_name in (
                pair
                for left_column, right_column in pairs
                for pair in (
                    (left_endpoint.casefold(), left_column),
                    (right_endpoint.casefold(), right_column),
                )
            ):
                model = models.get(table_name)
                if model is None:
                    problem = D("SST-REF001", origin=origin, subject=subject, model=table_name)
                    break
                if model.column(column_name) is None:
                    problem = D("SST-REF002", origin=origin, subject=subject, model=table_name, column=column_name)
                    break
        if problem is not None:
            diagnostics.append(problem)
            continue
        if pairs and len(pairs) == len(raw_conditions):
            out.append(
                (
                    Relationship(
                        name=str(node["name"]).upper(),
                        from_table=left_endpoint.upper(),
                        from_columns=tuple(left for left, _ in pairs),
                        to_table=right_endpoint.upper(),
                        to_columns=tuple(right for _, right in pairs),
                        asof_index=asof_index,
                        range_bounds=range_bounds,
                    ),
                    _node_origin(document, _member_root("relationship"), index),
                )
            )
    return tuple(out), tuple(diagnostics)
