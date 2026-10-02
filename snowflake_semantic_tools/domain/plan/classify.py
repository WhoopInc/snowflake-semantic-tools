"""Decide each rendered artifact's action and reason, and the diagnostics the decision reports.

`classify` takes the first of these that applies to an artifact:

1. its own validation errors block it; they travel on the change, not in the plan's list;
2. a composite lifecycle handler's plan decides it, and the plan reports what the handler found;
3. nothing is observed under its key, so it is created;
4. an object of another type holds its name, which blocks it (SST-PLN002);
5. no state entry records the object, which blocks it as unmanaged (SST-PLN024);
6. the first row of `_STATE_RULES` whose condition holds, comparing the object with the
   entry apply recorded for it; an object no row matches is unchanged.

A blocking diagnostic travels on the change as well as in the plan's list. Two warnings
never change a decision: a declared name whose case differs from the live object's
(SST-PLN023), reported before the decision's own diagnostic, and an update of an object
that carries explicit grants (SST-PLN013), reported after it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    CompositePlan,
    ObservedArtifact,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.model.registry import Registry
from snowflake_semantic_tools.domain.state import PARTIAL_WRITE, AppliedEntry


@dataclass(frozen=True, slots=True)
class Decision:
    """One artifact's change, and the diagnostics deciding it reported for the plan's list.

    Attributes:
        diagnostics: What the plan reports for the artifact, in order. A blocking diagnostic
            is also on the change; an artifact's own validation errors are only there.
    """

    change: Change
    diagnostics: tuple[Diagnostic, ...] = ()


def classify(
    key: str,
    artifact: RenderedArtifact,
    registry: Registry,
    *,
    observed: ObservedArtifact | None,
    recorded: AppliedEntry | None,
    manifest_id: str,
    validation: DiagnosticBag,
    composite: CompositePlan | None,
) -> Decision:
    """Decide one rendered artifact's change, and the diagnostics the decision reports.

    The first rule the module docstring lists that applies decides.

    Args:
        key: The key plan indexes the artifact by, which every diagnostic names it by.
        observed: The object Snowflake shows under the key; None when there is none.
        recorded: The entry state recorded when apply last wrote the key; None when there is none.
        manifest_id: The id of the manifest being planned.
        validation: What validation reported for the artifact; an error blocks it.
        composite: What the artifact's composite lifecycle handler planned; None for an
            artifact the generic lifecycle plans.

    Raises:
        KeyError: the registry has no entry for the artifact's type.

    Diagnostics:
        SST-PLN002: an object of another type holds the artifact's name.
        SST-PLN013: an update replaces an object that carries explicit grants.
        SST-PLN014: the object changed after apply recorded it.
        SST-PLN023: the declared name's case differs from the live object's.
        SST-PLN024: an object holds the name, but state records no entry for it.
        SST-PLN025: state records the artifact at another target.
    """
    artifact_type = registry.artifacts[artifact.artifact_type]
    if validation.has_errors:
        return _invalid(key, artifact, observed, artifact_type.ddl_position, validation)
    if composite is not None:
        return _handled(key, artifact, artifact_type.ddl_position, composite)
    if observed is None:
        return Decision(_change(artifact, None, Action.CREATE, ChangeReason.NOT_PRESENT, registry))
    if observed.object_type.upper() != (artifact.object_type or artifact_type.object_type).upper():
        diagnostic = D("SST-PLN002", artifact=key, value=observed.qualified_name.sql, found=observed.object_type)
        blocked = _change(artifact, observed, Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, registry, (diagnostic,))
        return Decision(blocked, (diagnostic,))
    return _compare(key, artifact, observed, recorded, manifest_id, registry)


def _invalid(
    key: str,
    artifact: RenderedArtifact,
    observed: ObservedArtifact | None,
    order: int,
    validation: DiagnosticBag,
) -> Decision:
    """Block an artifact validation failed, carrying it as rendered and its validation report."""
    return Decision(
        Change(
            key,
            artifact.artifact_type,
            Action.BLOCKED,
            ChangeReason.VALIDATION_ERRORS,
            artifact,
            observed,
            artifact.depends_on,
            order,
            validation,
        )
    )


def _handled(key: str, artifact: RenderedArtifact, order: int, composite: CompositePlan) -> Decision:
    """Adopt a composite handler's plan; the change carries the artifact as rendered."""
    change = Change(
        key,
        artifact.artifact_type,
        composite.action,
        composite.reason,
        artifact,
        None,
        artifact.depends_on,
        order,
        composite.diagnostics,
        composite_observation=composite.observation,
    )
    return Decision(change, tuple(composite.diagnostics))


def _compare(
    key: str,
    artifact: RenderedArtifact,
    observed: ObservedArtifact,
    recorded: AppliedEntry | None,
    manifest_id: str,
    registry: Registry,
) -> Decision:
    """Decide an artifact whose name holds an object of its type, by what state recorded there."""
    reported: list[Diagnostic] = []
    if observed.raw_name != artifact.target.name.folded and not artifact.target.name.quoted:
        reported.append(D("SST-PLN023", artifact=key, value=artifact.target.name.value, found=observed.raw_name))
    action, reason, blocking = _state_verdict(key, artifact, observed, recorded, manifest_id)
    change = _change(artifact, observed, action, reason, registry, blocking)
    reported.extend(blocking)
    if change.action is Action.UPDATE and observed.explicit_grants:
        reported.append(
            D("SST-PLN013", artifact=key, count=len(observed.explicit_grants), value=observed.qualified_name.sql)
        )
    return Decision(change, tuple(reported))


@dataclass(frozen=True, slots=True)
class _Evidence:
    """An observed object beside the state entry apply recorded for it: what a state rule reads."""

    key: str
    artifact: RenderedArtifact
    observed: ObservedArtifact
    recorded: AppliedEntry
    manifest_id: str


@dataclass(frozen=True, slots=True)
class _StateRule:
    """One row of the state table: when `holds`, the artifact gets `action` for `reason`.

    `report` builds the diagnostic a blocking row reports and attaches to its change; it is
    None for a row that reports nothing.
    """

    holds: Callable[[_Evidence], bool]
    action: Action
    reason: ChangeReason
    report: Callable[[_Evidence], Diagnostic] | None = None


def _recorded_elsewhere(evidence: _Evidence) -> bool:
    return QualifiedName.parse(evidence.recorded.qualified_name).folded != evidence.artifact.target.folded


def _changed_out_of_band(evidence: _Evidence) -> bool:
    # Only a marker naming the recorded manifest and fingerprint, on an object under the
    # recorded name, proves the live object is still the one apply wrote.
    observed, recorded = evidence.observed, evidence.recorded
    return (
        QualifiedName.parse(recorded.qualified_name).folded != observed.qualified_name.folded
        or observed.marker is None
        or observed.marker.manifest_id != recorded.manifest_id
        or observed.marker.fingerprint != recorded.fingerprint
    )


def _applied_under_another_manifest(evidence: _Evidence) -> bool:
    return evidence.recorded.manifest_id != evidence.manifest_id


def _fingerprint_differs(evidence: _Evidence) -> bool:
    return evidence.recorded.fingerprint != evidence.artifact.fingerprint


def _written_partly(evidence: _Evidence) -> bool:
    # A publish whose statements stopped part way recorded the fingerprint it meant to
    # publish, not one it finished, so the object is updated rather than trusted as unchanged.
    return evidence.recorded.outcome == PARTIAL_WRITE


def _target_moved(evidence: _Evidence) -> Diagnostic:
    return D(
        "SST-PLN025",
        artifact=evidence.key,
        found=evidence.recorded.qualified_name,
        expected=evidence.artifact.target.sql,
    )


def _out_of_band(evidence: _Evidence) -> Diagnostic:
    return D("SST-PLN014", artifact=evidence.key)


# The first row that holds decides, and an object no row matches is unchanged. The blocking
# rows come first, so an update never overwrites an object that moved or changed out of band.
_STATE_RULES: tuple[_StateRule, ...] = (
    _StateRule(_recorded_elsewhere, Action.BLOCKED, ChangeReason.TARGET_MOVED, _target_moved),
    _StateRule(_changed_out_of_band, Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, _out_of_band),
    _StateRule(_applied_under_another_manifest, Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH),
    _StateRule(_fingerprint_differs, Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS),
    _StateRule(_written_partly, Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS),
)


def _state_verdict(
    key: str,
    artifact: RenderedArtifact,
    observed: ObservedArtifact,
    recorded: AppliedEntry | None,
    manifest_id: str,
) -> tuple[Action, ChangeReason, tuple[Diagnostic, ...]]:
    """Decide an object of the artifact's type by the entry apply recorded for it.

    Returns:
        The action, its reason, and the diagnostic that blocks it; empty when nothing blocks.
    """
    if recorded is None:
        unmanaged = D("SST-PLN024", artifact=key, value=observed.qualified_name.sql)
        return Action.BLOCKED, ChangeReason.UNMANAGED_OBJECT, (unmanaged,)
    evidence = _Evidence(key, artifact, observed, recorded, manifest_id)
    for rule in _STATE_RULES:
        if rule.holds(evidence):
            return rule.action, rule.reason, () if rule.report is None else (rule.report(evidence),)
    return Action.NOOP, ChangeReason.UNCHANGED, ()


def _change(
    artifact: RenderedArtifact,
    observed: ObservedArtifact | None,
    action: Action,
    reason: ChangeReason,
    registry: Registry,
    diagnostics: tuple[Diagnostic, ...] = (),
) -> Change:
    """Build an action's change, carrying the artifact narrowed to the statements the action runs."""
    effective = artifact.for_action(action, observed)
    return Change(
        effective.key,
        effective.artifact_type,
        action,
        reason,
        effective,
        observed,
        effective.depends_on,
        registry.artifacts[effective.artifact_type].ddl_position,
        DiagnosticBag(diagnostics),
    )
