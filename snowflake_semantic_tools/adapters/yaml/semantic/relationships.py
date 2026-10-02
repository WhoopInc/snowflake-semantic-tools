"""Read relationships, and check them against each other and the views that hold their tables."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _load_nodes, _member_root, _node_origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.reference import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.model.sql_checks import name_problem


def _relationship_diagnostics(
    relationships: tuple[Relationship, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
    origins: Mapping[str, Origin] | None = None,
    models: Mapping[str, DbtModel] | None = None,
) -> tuple[Diagnostic, ...]:
    """Check each relationship against the views that hold its tables and the keys of its right table.

    A relationship whose two tables no one view holds is reported against the view holding the
    most of them (the first such view, or `semantic_view:<none>` without views), naming the
    first table in name order that view lacks, and is checked no further. Otherwise, when
    `models` has its right table and it is neither an ASOF nor a range join, that table's keys
    are checked. Names compare casefolded; each subject is the relationship's casefolded key,
    and each origin is the one `origins` holds under its casefolded name.

    Diagnostics:
        SST-VAL203: when no view holds both of the relationship's tables.
        SST-MEM005: with each SST-VAL203, since the relationship then attaches to no view.
        SST-VAL311: when the right table declares neither `primary_key` nor `unique_keys`.
        SST-VAL210: when no key of the right table lies within the join's right-hand columns.
    """
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
    """Report each table pair two or more relationships join, and a metric those paths leave ambiguous.

    Pairs are unordered and casefolded, and are reported in sorted order.

    Diagnostics:
        SST-VAL209: for each view that holds both tables of such a pair.
        SST-VAL116: for the first metric, in order, whose first table is in the pair and that
            declares no `using_relationships`; no other metric is reported for that pair.
    """
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
    Each is parsed, checked and built by `_read_relationship`. One with no name,
    no list of conditions or a legacy global is left out without a word here:
    SST-PRS107, SST-VAL201 and SST-REF034/SST-REF035 report those.

    Diagnostics:
        SST-REF045: when an endpoint is written as a `{{ ref() }}` call.
        SST-PRS110: when a condition is not an equality, an ASOF comparison or a range.
        SST-VAL204: when a condition's column is on a table other than its side's endpoint.
        SST-REF001: when an endpoint names no dbt model.
        SST-REF002: when a condition names a column its endpoint's model does not have.
        SST-PRS005: when the name, an endpoint, or a column is not a valid identifier.
    """
    root = project_dir / semantic_models_dir / "relationships"
    out: list[tuple[Relationship, Origin]] = []
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, root, _member_root("relationship")):
        raw_conditions = node.get("relationship_conditions")
        if not node.get("name") or not isinstance(raw_conditions, list) or not raw_conditions:
            continue
        if any(_uses_legacy_globals(condition) for condition in raw_conditions):
            # SST-REF034/SST-REF035 report the legacy globals; parsing further
            # would only bury that diagnostic under a condition-shape error.
            continue
        origin = _node_origin(document, _member_root("relationship"), index)
        read = _read_relationship(node, raw_conditions, origin, models)
        if isinstance(read, Relationship):
            out.append((read, origin))
        else:
            diagnostics.extend(read)
    return tuple(out), tuple(diagnostics)


_REF = r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}"
_EQUALITY = re.compile(rf"^\s*{_REF}\s*=\s*{_REF}\s*$")
_ASOF = re.compile(rf"^\s*{_REF}\s*>=\s*{_REF}\s*$")
_RANGE = re.compile(rf"^\s*{_REF}\s+BETWEEN\s+{_REF}\s+AND\s+{_REF}\s*$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class _Condition:
    """One relationship condition, parsed: the column each side names and the kind of join it makes."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str
    asof: bool = False
    range_bounds: tuple[str, str] | None = None


@dataclass(frozen=True, slots=True)
class _Conditions:
    """A relationship's conditions, parsed: its column pairs, upper-cased, and its temporal modifiers."""

    pairs: tuple[tuple[str, str], ...]
    asof_index: int | None
    range_bounds: tuple[str, str] | None


def _read_relationship(
    node: Mapping[str, Any],
    raw_conditions: list[Any],
    origin: Origin,
    models: Mapping[str, DbtModel] | None,
) -> Relationship | tuple[Diagnostic, ...]:
    """Parse, check and build one relationship, or return why it is left out.

    The steps run in order and each stops at its first problem: endpoints, then conditions in
    order, then (when `models` is given) the columns each pair names.

    Diagnostics:
        SST-REF045: when an endpoint is written as a `{{ ref() }}` call.
        SST-PRS110: when a condition is not an equality, an ASOF comparison or a range.
        SST-VAL204: when a condition's column is on a table other than its side's endpoint.
        SST-REF001: when an endpoint names no dbt model.
        SST-REF002: when a condition names a column its endpoint's model does not have.
        SST-PRS005: when the name, an endpoint, or a column is not a valid identifier.
    """
    subject = artifact_key("relationship", node["name"])
    endpoint_problems = _endpoint_diagnostics(node, subject, origin)
    if endpoint_problems:
        return endpoint_problems
    endpoints = (str(node.get("left_table")).strip(), str(node.get("right_table")).strip())
    conditions = _parse_conditions(raw_conditions, endpoints, node["name"], origin, subject)
    if isinstance(conditions, Diagnostic):
        return (conditions,)
    problem = _column_problem(conditions.pairs, endpoints, models, origin, subject) if models is not None else None
    if problem is not None:
        return (problem,)
    left_endpoint, right_endpoint = endpoints
    names = (
        str(node["name"]),
        left_endpoint,
        right_endpoint,
        *(column for pair in conditions.pairs for column in pair),
        *(conditions.range_bounds or ()),
    )
    invalid = next(
        (
            found
            for found in (name_problem(name, artifact=subject, subject=subject, origin=origin) for name in names)
            if found
        ),
        None,
    )
    if invalid is not None:
        return (invalid,)
    return Relationship(
        name=str(node["name"]).upper(),
        from_table=left_endpoint.upper(),
        from_columns=tuple(left for left, _ in conditions.pairs),
        to_table=right_endpoint.upper(),
        to_columns=tuple(right for _, right in conditions.pairs),
        asof_index=conditions.asof_index,
        range_bounds=conditions.range_bounds,
    )


def _parse_conditions(
    raw_conditions: list[Any], endpoints: tuple[str, str], name: object, origin: Origin, subject: str
) -> _Conditions | Diagnostic:
    """Parse every condition in order, or return the first one that is malformed or misplaced.

    The last ASOF condition sets the ASOF column and the last range sets the range bounds.

    Diagnostics:
        SST-PRS110: when a condition is not an equality, an ASOF comparison or a range.
        SST-VAL204: when a condition's column is on a table other than its side's endpoint.
    """
    pairs: list[tuple[str, str]] = []
    asof_index: int | None = None
    range_bounds: tuple[str, str] | None = None
    for condition in raw_conditions:
        parsed = _parse_condition(condition)
        if parsed is None:
            return D("SST-PRS110", origin=origin, subject=subject, artifact=subject, value=condition)
        mismatch = _endpoint_mismatch(parsed, endpoints)
        if mismatch is not None:
            table, column, endpoint = mismatch
            return D(
                "SST-VAL204",
                origin=origin,
                subject=subject,
                relationship=name,
                column=f"{table}.{column}",
                name=endpoint,
            )
        if parsed.asof:
            asof_index = len(pairs)
        if parsed.range_bounds is not None:
            range_bounds = parsed.range_bounds
        pairs.append((parsed.left_column.upper(), parsed.right_column.upper()))
    return _Conditions(tuple(pairs), asof_index, range_bounds)


def _parse_condition(condition: object) -> _Condition | None:
    """Parse one condition as an equality, an ASOF comparison or a range, trying them in that order.

    A range whose two bounds are on different tables parses to None, like any other shape.
    """
    match = _EQUALITY.fullmatch(str(condition))
    if match is not None:
        left_table, left_column, right_table, right_column = match.groups()
        return _Condition(left_table, left_column, right_table, right_column)
    match = _ASOF.fullmatch(str(condition))
    if match is not None:
        left_table, left_column, right_table, right_column = match.groups()
        return _Condition(left_table, left_column, right_table, right_column, asof=True)
    match = _RANGE.fullmatch(str(condition))
    if match is None or match.group(3).casefold() != match.group(5).casefold():
        return None
    left_table, left_column, right_table, range_start, _range_table, range_end = match.groups()
    return _Condition(
        left_table, left_column, right_table, range_start, range_bounds=(range_start.upper(), range_end.upper())
    )


def _endpoint_mismatch(condition: _Condition, endpoints: tuple[str, str]) -> tuple[str, str, str] | None:
    """Return the first side, left then right, whose column is not on that side's endpoint."""
    return next(
        (
            (table, column, endpoint)
            for table, column, endpoint in (
                (condition.left_table, condition.left_column, endpoints[0]),
                (condition.right_table, condition.right_column, endpoints[1]),
            )
            if table.casefold() != endpoint.casefold()
        ),
        None,
    )


def _column_problem(
    pairs: tuple[tuple[str, str], ...],
    endpoints: tuple[str, str],
    models: Mapping[str, DbtModel],
    origin: Origin,
    subject: str,
) -> Diagnostic | None:
    """Return the first endpoint model or pair column, pair by pair and left side first, that is missing.

    Diagnostics:
        SST-REF001: when an endpoint names no dbt model.
        SST-REF002: when a pair names a column its endpoint's model does not have.
    """
    left_endpoint, right_endpoint = endpoints
    for left_column, right_column in pairs:
        for table_name, column_name in (
            (left_endpoint.casefold(), left_column),
            (right_endpoint.casefold(), right_column),
        ):
            model = models.get(table_name)
            if model is None:
                return D("SST-REF001", origin=origin, subject=subject, model=table_name)
            if model.column(column_name) is None:
                return D("SST-REF002", origin=origin, subject=subject, model=table_name, column=column_name)
    return None
