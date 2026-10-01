"""What the semantic load leaves out: the `Poison` value and the two phases that grow it.

A definition that a diagnostic proves broken is poisoned. A poisoned member attaches to
no view and a poisoned view is not built, so the load reports each fault once and still
builds everything that does not depend on it.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace

from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.adapters.yaml.semantic.phases import SemanticChecks, SemanticMembers
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.project import ParsedMember, ParsedView


@dataclass(frozen=True, slots=True)
class Poison:
    """The metrics, members and views the load leaves out, grown by copy as phases find more.

    Attributes:
        metric_names: Casefolded names of the metrics in a cycle or naming a table that is
            not a dbt model; with `metric_subjects`, what `healthy_metrics` leaves out.
        metric_subjects: Subjects of the metrics with an error diagnostic, exactly as the
            metric checks reported them (not casefolded).
        member_keys: Casefolded keys of the members that attach to no view.
        view_names: Casefolded names that more than one view declares; none of them is built.
        view_keys: Casefolded keys of the views that are checked but not built: an error
            names them, their tables are malformed, or their file uses the legacy globals.
    """

    metric_names: frozenset[str] = frozenset()
    metric_subjects: frozenset[str] = frozenset()
    member_keys: frozenset[str] = frozenset()
    view_names: frozenset[str] = frozenset()
    view_keys: frozenset[str] = frozenset()

    def healthy_metrics(self, metrics: tuple[MetricDef, ...]) -> tuple[MetricDef, ...]:
        """Keep, in order, the metrics sound enough to check against the relationships.

        A metric that only a later phase poisons stays healthy here: this reads
        `metric_names` and `metric_subjects`, which no phase after the member poison grows.
        """
        return tuple(
            metric
            for metric in metrics
            if metric.name.casefold() not in self.metric_names
            and artifact_key("metric", metric.name) not in self.metric_subjects
        )

    def with_members(self, keys: Iterable[str]) -> Poison:
        """Poison these casefolded member keys as well."""
        return replace(self, member_keys=self.member_keys | frozenset(keys))


def _error_subjects(diagnostics: Iterable[Diagnostic]) -> frozenset[str]:
    """Collect the subject of every error among `diagnostics`, exactly as reported."""
    return frozenset(
        diagnostic.subject
        for diagnostic in diagnostics
        if diagnostic.subject is not None and diagnostic.severity is Severity.ERROR
    )


def _member_poison(
    parsed_members: tuple[ParsedMember, ...],
    members: SemanticMembers,
    legacy_files: frozenset[str],
    semantic: SemanticChecks,
) -> Poison:
    """Poison what the typed members and the structural and semantic checks prove broken.

    Poisoned: every metric in a cycle; every metric, filter and verified query the metric
    or expression checks report an error for, read from their unfiltered findings so a
    legacy file's SST-REF034/SST-REF035 still count; every filter and verified query naming
    a table that is not a dbt model; every member of a file that uses the legacy globals.
    A metric naming such a table is not poisoned, only left out of the healthy metrics.
    """
    cycle_names = frozenset(name for cycle in members.cycles for name in cycle[:-1])
    metric_subjects = _error_subjects(semantic.metric_findings)
    member_keys = {subject.casefold() for subject in metric_subjects}
    member_keys.update(artifact_key("metric", name) for name in cycle_names)
    member_keys.update(subject.casefold() for subject in _error_subjects(semantic.expression_findings))
    member_keys.update(semantic.unknown_table_members)
    member_keys.update(member.key for member in parsed_members if member.origin.file in legacy_files)
    return Poison(
        metric_names=cycle_names | semantic.unknown_table_metrics,
        metric_subjects=metric_subjects,
        member_keys=frozenset(member_keys),
    )


def _view_poison(
    poison: Poison,
    views: tuple[ParsedView, ...],
    reported: tuple[Diagnostic, ...],
    duplicate_views: frozenset[str],
    legacy_files: frozenset[str],
) -> Poison:
    """Poison every view an error so far names, whose name repeats, or that cannot be read.

    `reported` must hold every diagnostic of the phases before this one: an error whose
    subject is a view poisons that view, whichever check reported it, so a check that runs
    after this phase cannot keep its view from being built. A view whose tables are
    malformed is poisoned too, and so is every view of a legacy file.
    """
    view_prefix = artifact_key("semantic_view", "")
    view_keys = {
        subject.casefold() for subject in _error_subjects(reported) if subject.casefold().startswith(view_prefix)
    }
    view_keys.update(artifact_key("semantic_view", view.name).casefold() for view in views if view.poisoned)
    # A view authored with the legacy globals is rejected by SST-REF034; building
    # it would only report the same call again as an internal error.
    view_keys.update(
        artifact_key("semantic_view", view.name).casefold() for view in views if view.origin.file in legacy_files
    )
    return replace(poison, view_names=duplicate_views, view_keys=frozenset(view_keys))
