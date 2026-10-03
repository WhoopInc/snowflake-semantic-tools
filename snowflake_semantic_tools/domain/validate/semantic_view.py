"""Check a built semantic view and the statement that publishes it: guards, and its join graph.

The loader checks what was authored; these run on what compile produced, so a defect in SST's
own build or renderer cannot reach Snowflake unreported. Each takes the view as the model holds
it and returns diagnostics; none raises for a user's project.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind, SemanticView, Table

# The derived-metric restrictions Snowflake enforces at neither validate nor create time.
CRITICAL_METRIC_CODES = frozenset(("SST-VAL102", "SST-VAL103", "SST-VAL104", "SST-VAL105", "SST-VAL106", "SST-VAL107"))


def statement_diagnostics(ddl: str, *, artifact: str) -> tuple[Diagnostic, ...]:
    """Check the statement that would publish a view for what a publisher must never do.

    Diagnostics:
        SST-VAL307: the statement replaces the view without COPY GRANTS, which drops its grants.
        SST-VAL321: a CREATE OR ALTER statement sets tags, which that statement cannot do.
    """
    head = " ".join(ddl.split()[:4]).upper()
    lines = [line.strip().upper() for line in ddl.splitlines()]
    diagnostics: list[Diagnostic] = []
    if head.startswith("CREATE OR REPLACE") and "COPY GRANTS" not in lines:
        diagnostics.append(D("SST-VAL307", subject=artifact, artifact=artifact))
    if head.startswith("CREATE OR ALTER") and "WITH TAG (" in lines:
        diagnostics.append(D("SST-VAL321", subject=artifact, artifact=artifact))
    return tuple(diagnostics)


def restriction_diagnostics(
    view: SemanticView, reported: Iterable[Diagnostic], *, artifact: str
) -> tuple[Diagnostic, ...]:
    """Report each metric a view would publish although a derived-metric restriction failed for it.

    A metric the restrictions reject is never attached, so this reports only a build that
    attached one anyway.

    Diagnostics:
        SST-VAL123: a metric with a critical-set error is in the view.
    """
    failed = {
        item.subject.casefold()
        for item in reported
        if item.code in CRITICAL_METRIC_CODES and item.blocks and item.subject
    }
    return tuple(
        D("SST-VAL123", subject=artifact, metric=metric.name.casefold())
        for metric in sorted(view.metrics, key=lambda item: item.name)
        if artifact_key("metric", metric.name).casefold() in failed
    )


def relation_diagnostics(
    tables: Iterable[Table], relations: Mapping[str, str], *, artifact: str
) -> tuple[Diagnostic, ...]:
    """Report each table that would render against anything but the relation its model resolves to.

    `relations` maps each logical table name to its dbt model's resolved relation. A model
    with an alias names a relation other than the model, and the view must point at that one.

    Diagnostics:
        SST-VAL302: a table's three-part name is not its model's resolved relation.
    """
    return tuple(
        D("SST-VAL302", subject=artifact, artifact=artifact, name=table.logical_name.casefold(), value=relation)
        for table in tables
        if (relation := relations.get(table.logical_name)) is not None
        and QualifiedName.parse(table.fqn).folded != QualifiedName.parse(relation).folded
    )


def join_graph_diagnostics(view: SemanticView, *, artifact: str) -> tuple[Diagnostic, ...]:
    """Report the shape of a view's join graph, then each many-to-many path through a bridge table.

    A bridge is a table on the many side of two relationships to two other tables: through it,
    each of those tables joins many rows of the other. Each pair is reported once.

    Diagnostics:
        SST-VAL217: the view's tables, relationships, and how many connected parts they form.
        SST-VAL216: two tables are joined many-to-many through a bridge table.
    """
    scope = view.scope
    relationships = [item for item in view.relationships if scope.admits_relationship(item.name)]
    if not relationships:
        return ()
    tables = [table.logical_name for table in view.tables]
    parts = _components(tables, ((item.from_table, item.to_table) for item in relationships))
    shape = f"{len(relationships)} relationships in {parts} connected part{'s' if parts != 1 else ''}"
    diagnostics = [D("SST-VAL217", subject=artifact, artifact=artifact, count=len(tables), value=shape)]
    targets: dict[str, list[str]] = {}
    for item in sorted(relationships, key=lambda relationship: relationship.name):
        if (
            item.asof_index is None
            and item.range_bounds is None
            and item.to_table not in targets.get(item.from_table, [])
        ):
            targets.setdefault(item.from_table, []).append(item.to_table)
    for bridge in sorted(targets):
        joined = sorted(targets[bridge])
        for index, left in enumerate(joined):
            for right in joined[index + 1 :]:
                diagnostics.append(
                    D("SST-VAL216", subject=artifact, artifact=artifact, value=f"{left} <-> {right}", name=bridge)
                )
    return tuple(diagnostics)


def _components(nodes: list[str], edges: Iterable[tuple[str, str]]) -> int:
    parent = {node: node for node in nodes}

    def root(node: str) -> str:
        while parent.setdefault(node, node) != node:
            node = parent[node]
        return node

    for left, right in edges:
        parent[root(left)] = root(right)
    return len({root(node) for node in parent})


def fan_out_diagnostics(views: Iterable[tuple[SemanticView, str]]) -> tuple[Diagnostic, ...]:
    """Report how far implicit attachment reached: each view's members, then each shared metric.

    Every member counted attaches by the tables it needs, so the authored files cannot show
    where it lands. Views are reported in the order given; metrics by name.

    Diagnostics:
        SST-VAL319: what each view holds by table membership, by type; a view holding nothing
            that way is not reported.
        SST-VAL125: a metric is in more than one view.
    """
    pairs = tuple(views)
    diagnostics = [
        D("SST-VAL319", subject=key, artifact=key, value=value)
        for view, key in pairs
        if (value := _members_value(view)) is not None
    ]
    reach: dict[str, int] = {}
    for view, _ in pairs:
        for metric in view.metrics:
            if view.scope.admits_metric(metric.name):
                reach[metric.name.casefold()] = reach.get(metric.name.casefold(), 0) + 1
    diagnostics.extend(
        D("SST-VAL125", subject=artifact_key("metric", name), metric=name, count=count)
        for name, count in sorted(reach.items())
        if count > 1
    )
    return tuple(diagnostics)


def _members_value(view: SemanticView) -> str | None:
    """Count what a view holds by table membership, in clause order; None when it holds nothing."""
    scope = view.scope
    counts = {
        "relationship": sum(1 for item in view.relationships if scope.admits_relationship(item.name)),
        "filter": sum(1 for column in view.columns if column.kind is ColumnKind.FILTER),
        "metric": sum(1 for metric in view.metrics if scope.admits_metric(metric.name)),
        "verified query": len(view.verified_queries),
    }
    words = [f"{count} {kind if count == 1 else _plural(kind)}" for kind, count in counts.items() if count]
    return ", ".join(words) + " by table membership" if words else None


def _plural(word: str) -> str:
    return word[:-1] + "ies" if word.endswith("y") else word + "s"
