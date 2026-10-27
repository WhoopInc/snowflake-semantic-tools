"""Membership checks over the built views: what attachment placed, against what each view renders.

Attachment decides once which members a view holds, and the renderer renders every one of
them. These checks hold the built views to that: an attached member a view does not render,
and a metric whose synonym names something else in its view too, are both reported before
any DDL is sent.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.project import ArtifactKey, ParsedMember
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind, SemanticView
from snowflake_semantic_tools.domain.resolve.membership_model import Attachment


def rendered_diagnostics(
    built: Mapping[ArtifactKey, SemanticView], members: tuple[ParsedMember, ...], attachment: Attachment
) -> tuple[Diagnostic, ...]:
    """Check each built view against the members attached to it, view by view in key order.

    Args:
        built: Each view that built, by its artifact key.

    Diagnostics:
        SST-MEM014: an attached metric, filter, relationship, verified query or custom
            instruction is not in the view it attached to.
        SST-MEM009: a metric shares a synonym with another table, column or metric of its view.
    """
    diagnostics: list[Diagnostic] = []
    for key in sorted(built):
        view = built[key]
        rendered = _rendered_names(view)
        for member in members:
            names = rendered.get(member.type_name)
            if names is None or key not in attachment.get(member.key, ()):
                continue
            filter_prose = (
                member.type_name == "filter" and member.name.casefold() in (view.ai_sql_generation or "").casefold()
            )
            if member.name.casefold() not in names and not filter_prose:
                diagnostics.append(
                    D("SST-MEM014", member=member.key, artifact=key, subject=member.key, origin=member.origin)
                )
        diagnostics.extend(_synonym_clashes(key, view))
    return tuple(diagnostics)


def _rendered_names(view: SemanticView) -> dict[str, frozenset[str]]:
    """The casefolded names of what a view renders, by the member type each comes from."""
    return {
        "metric": frozenset(metric.name.casefold() for metric in view.metrics),
        "filter": frozenset(column.name.casefold() for column in view.columns if column.kind is ColumnKind.FILTER),
        "relationship": frozenset(relationship.name.casefold() for relationship in view.relationships),
        "verified_query": frozenset(query.name.casefold() for query in view.verified_queries),
        "custom_instruction": frozenset(name.casefold() for name in view.custom_instruction_names),
    }


def _claimants(view: SemanticView) -> Iterator[tuple[str, tuple[str, ...], bool]]:
    """Yield what in a view carries synonyms, by its rendered name, and whether it is a metric."""
    for table in view.tables:
        yield table.logical_name, table.synonyms, False
    for column in view.columns:
        yield f"{column.table}.{column.name}", column.synonyms, False
    for metric in view.metrics:
        yield (f"{metric.table}.{metric.name}" if metric.table else metric.name), metric.synonyms, True


def _synonym_clashes(key: ArtifactKey, view: SemanticView) -> list[Diagnostic]:
    """Report each synonym a metric shares with another claimant of its view, casefolded (SST-MEM009).

    A metric's synonym is what a question names it by, so it must pick out one thing in the
    view. A table and its columns may share one -- a foreign key and the key it references
    describe one entity -- so a clash between two non-metrics is not reported.
    """
    owners: dict[str, list[tuple[str, bool]]] = {}
    diagnostics: list[Diagnostic] = []
    for claimant, synonyms, is_metric in _claimants(view):
        for synonym in dict.fromkeys(synonyms):
            earlier = owners.setdefault(synonym.casefold(), [])
            clash = next((name for name, metric in earlier if name != claimant and (metric or is_metric)), None)
            if clash is not None:
                diagnostics.append(D("SST-MEM009", artifact=key, value=synonym, a=clash, b=claimant, subject=key))
            earlier.append((claimant, is_metric))
    return diagnostics
