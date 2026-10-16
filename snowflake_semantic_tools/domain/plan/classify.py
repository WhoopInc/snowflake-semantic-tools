"""Decide each rendered artifact's action and reason, and the diagnostics the decision reports.

`classify` takes the first of these that applies to an artifact:

1. its own validation errors block it; they travel on the change, not in the plan's list;
2. a composite lifecycle handler's plan decides it, and the plan reports what the handler found;
3. nothing is observed under its key, so it is created;
4. an object of another type holds its name, which blocks it (SST-PLN002);
5. no state entry records the object, which blocks it as unmanaged (SST-PLN024);
6. the first row of `_STATE_RULES` whose condition holds, comparing the object -- its marker,
   and the digest of the properties the marker does not prove -- with the entry apply
   recorded for it; an object no row matches is unchanged.

Each artifact is decided by its own entry alone. The fingerprint digests everything its
rendered form carries, resolved dependencies included, and the entry and the live marker
name the manifest that last published it, so an artifact whose fingerprint matches is
unchanged whatever else the project, or the manifest that last stamped state, changed.

A blocking diagnostic travels on the change as well as in the plan's list. Three warnings
never change a decision: a declared name whose case differs from the live object's
(SST-PLN023), reported before the decision's own diagnostic; and an update of an object that
carries explicit grants (SST-PLN013), then of a semantic view that carries tags the project
does not declare (SST-PLN039), reported after it. An agent updated for version drift reports
what drifted (SST-PLN102) after them.

An agent's ownership marker means "SST is writing this definition": publishing writes the
marked comment before anything else, so an apply that stops part way leaves the marker state
records for the write, and the partial write is finished rather than refused as changed out
of band.
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
    drifted_properties,
    version_drift,
)
from snowflake_semantic_tools.domain.model.registry import Registry
from snowflake_semantic_tools.domain.state import PARTIAL_WRITE, AppliedEntry
from snowflake_semantic_tools.domain.validate.shared import is_digest


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
    validation: DiagnosticBag,
    composite: CompositePlan | None,
) -> Decision:
    """Decide one rendered artifact's change, and the diagnostics the decision reports.

    The first rule the module docstring lists that applies decides.

    Args:
        key: The key plan indexes the artifact by, which every diagnostic names it by.
        observed: The object Snowflake shows under the key; None when there is none.
        recorded: The entry state recorded when apply last wrote the key; None when there is none.
        validation: What validation reported for the artifact; an error blocks it.
        composite: What the artifact's composite lifecycle handler planned; None for an
            artifact the generic lifecycle plans.

    Raises:
        KeyError: the registry has no entry for the artifact's type.

    Diagnostics:
        SST-PLN002: an object of another type holds the artifact's name.
        SST-PLN013: an update replaces an object that carries explicit grants.
        SST-PLN014: the object changed after apply recorded it: its marker, or a property apply
            read back after writing it.
        SST-PLN023: the declared name's case differs from the live object's.
        SST-PLN024: an object holds the name, but state records no entry for it.
        SST-PLN025: state records the artifact at another target.
        SST-PLN039: an update replaces a semantic view that carries tags the project does not
            declare, which the replace removes.
        SST-PLN102: an otherwise unchanged agent's versions drifted from what SST published.
        SST-VAL320: the recorded or rendered fingerprint is not a canonical digest, so the
            comparison would not be of normalised definitions.
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
    return _compare(key, artifact, observed, recorded, registry)


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
    registry: Registry,
) -> Decision:
    """Decide an artifact whose name holds an object of its type, by what state recorded there."""
    reported: list[Diagnostic] = []
    if observed.raw_name != artifact.target.name.folded and not artifact.target.name.quoted:
        reported.append(D("SST-PLN023", artifact=key, value=artifact.target.name.value, found=observed.raw_name))
    action, reason, blocking = _state_verdict(key, artifact, observed, recorded)
    change = _change(artifact, observed, action, reason, registry, blocking)
    reported.extend(blocking)
    if change.action is Action.UPDATE and observed.explicit_grants:
        reported.append(
            D("SST-PLN013", artifact=key, count=len(observed.explicit_grants), value=observed.qualified_name.sql)
        )
    if change.action is Action.UPDATE and (removed := _removed_tags(artifact, observed)):
        reported.append(D("SST-PLN039", artifact=key, value=observed.qualified_name.sql, found=", ".join(removed)))
    if change.reason is ChangeReason.VERSION_DRIFT:
        reported.append(D("SST-PLN102", artifact=key, detail="; ".join(version_drift(artifact, observed))))
    return Decision(change, tuple(reported))


def _removed_tags(artifact: RenderedArtifact, observed: ObservedArtifact) -> tuple[str, ...]:
    """Return the tags on an observed semantic view that its replace does not set, casefolded alike."""
    # A replace sets exactly the tags its WITH TAG names; an agent's update unsets the others
    # itself, in statements the plan shows.
    if observed.object_type.upper() != "SEMANTIC VIEW":
        return ()
    declared = {tag.casefold() for tag in artifact.desired_tags}
    return tuple(tag for tag in sorted(observed.tags or ()) if tag.casefold() not in declared)


@dataclass(frozen=True, slots=True)
class _Evidence:
    """An observed object beside the state entry apply recorded for it: what a state rule reads."""

    key: str
    artifact: RenderedArtifact
    observed: ObservedArtifact
    recorded: AppliedEntry


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


def _unnormalised(evidence: _Evidence) -> bool:
    # Drift is decided by comparing digests of the canonical definition. A recorded or rendered
    # value that is not one would compare raw text, which differs for reasons that are not drift.
    return not (is_digest(evidence.recorded.fingerprint) and is_digest(evidence.artifact.fingerprint))


def _raw_comparison(evidence: _Evidence) -> Diagnostic:
    return D("SST-VAL320", artifact=evidence.key)


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


def _published_unrecorded(evidence: _Evidence) -> bool:
    # An apply stopped between publishing an object and recording it -- killed outright, or its
    # machine lost -- leaves SST's marker for exactly the definition being planned on an object
    # state records an earlier write of. Publishing that definition again changes nothing live
    # and records it, so the object is updated rather than refused as changed out of band.
    observed = evidence.observed
    return (
        _changed_out_of_band(evidence)
        and QualifiedName.parse(evidence.recorded.qualified_name).folded == observed.qualified_name.folded
        and observed.marker is not None
        and observed.marker.fingerprint == evidence.artifact.fingerprint
    )


def _properties_drifted(evidence: _Evidence) -> bool:
    # A marker only proves which definition SST wrote. An edit that kept the comment -- an
    # ALTER, or a replace from the object's own GET_DDL -- shows only in the properties apply
    # read back after writing, so one of those that differs is as out of band as the marker.
    return bool(drifted_properties(evidence.recorded.live_digest, evidence.observed.live_digest))


def _fingerprint_differs(evidence: _Evidence) -> bool:
    return evidence.recorded.fingerprint != evidence.artifact.fingerprint


def _written_partly(evidence: _Evidence) -> bool:
    # A publish whose statements stopped part way recorded the fingerprint it meant to
    # publish, not one it finished, so the object is updated rather than trusted as unchanged.
    return evidence.recorded.outcome == PARTIAL_WRITE


def _versions_drifted(evidence: _Evidence) -> bool:
    return bool(version_drift(evidence.artifact, evidence.observed))


def _target_moved(evidence: _Evidence) -> Diagnostic:
    return D(
        "SST-PLN025",
        artifact=evidence.key,
        found=evidence.recorded.qualified_name,
        expected=evidence.artifact.target.sql,
    )


def _out_of_band(evidence: _Evidence) -> Diagnostic:
    return D("SST-PLN014", artifact=evidence.key, detail="its ownership marker is not the one state records")


def _drifted(evidence: _Evidence) -> Diagnostic:
    changed = drifted_properties(evidence.recorded.live_digest, evidence.observed.live_digest)
    return D("SST-PLN014", artifact=evidence.key, detail=f"its {', '.join(changed)} changed; its marker did not")


# The first row that holds decides, and an object no row matches is unchanged. The blocking
# rows come first, so an update never overwrites an object that moved or changed out of band;
# the one update before them is of an object whose marker names the definition being planned.
# A property that drifted under an unchanged marker blocks next, before any update would
# replace the hand edit. Version drift comes last: it is checked only for an agent whose
# definition and newest committed specification are both as SST left them.
_STATE_RULES: tuple[_StateRule, ...] = (
    _StateRule(_unnormalised, Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, _raw_comparison),
    _StateRule(_recorded_elsewhere, Action.BLOCKED, ChangeReason.TARGET_MOVED, _target_moved),
    _StateRule(_published_unrecorded, Action.UPDATE, ChangeReason.UNRECORDED_PUBLISH),
    _StateRule(_changed_out_of_band, Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, _out_of_band),
    _StateRule(_properties_drifted, Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, _drifted),
    _StateRule(_fingerprint_differs, Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS),
    _StateRule(_written_partly, Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS),
    _StateRule(_versions_drifted, Action.UPDATE, ChangeReason.VERSION_DRIFT),
)


def _state_verdict(
    key: str,
    artifact: RenderedArtifact,
    observed: ObservedArtifact,
    recorded: AppliedEntry | None,
) -> tuple[Action, ChangeReason, tuple[Diagnostic, ...]]:
    """Decide an object of the artifact's type by the entry apply recorded for it.

    Returns:
        The action, its reason, and the diagnostic that blocks it; empty when nothing blocks.
    """
    if recorded is None:
        unmanaged = D("SST-PLN024", artifact=key, value=observed.qualified_name.sql)
        return Action.BLOCKED, ChangeReason.UNMANAGED_OBJECT, (unmanaged,)
    evidence = _Evidence(key, artifact, observed, recorded)
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
