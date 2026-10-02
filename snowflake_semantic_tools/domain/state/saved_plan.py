"""A saved plan: the changes a plan recorded, for a later apply to execute exactly as planned.

`SavedPlan.plan_id` is the `content_hash` of the document without the id, so
`SavedPlan.from_dict` refuses an edited plan. Each change also records what plan observed --
an ownership marker, or the hash of a composite handler's observation -- and apply re-plans
and refuses a saved plan whose observations or changes no longer match: `check_applicable`
says why, as a `PlanMismatch`.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import Change, ChangeSet
from snowflake_semantic_tools.domain.state.codec import (
    optional_object,
    pairs_from_json,
    pairs_to_json,
    resources_from_json,
    resources_to_json,
)
from snowflake_semantic_tools.domain.state.documents import content_hash

PLAN_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class SavedChange:
    """One change as a saved plan records it: what apply compares against a fresh plan.

    Attributes:
        action, reason: The change's `Action` and `ChangeReason`, by value.
        target: The object the change writes, or else the one it prunes; None when it has neither.
        fingerprint: The rendered artifact's fingerprint; None when there is no rendered artifact.
        previous_marker: The observed ownership marker's text, else the hash of what a composite
            handler observed; None when plan observed neither.
        statement_hashes: The SHA-256 hex digest of each statement apply would run, in order.
        component_fingerprints: The rendered artifact's component fingerprints, sorted.
        physical_resources: The rendered artifact's objects, as `(object type, qualified name)`.
    """

    key: str
    artifact_type: str
    action: str
    reason: str
    target: str | None
    fingerprint: str | None
    previous_marker: str | None
    statement_hashes: tuple[str, ...]
    depends_on: tuple[str, ...]
    order: int
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, str], ...] = ()
    prune_executable: bool = True

    @classmethod
    def from_change(cls, change: Change) -> SavedChange:
        """Record a planned change, hashing its statements and what plan observed."""
        return cls(
            key=change.key,
            artifact_type=change.artifact_type,
            action=change.action.value,
            reason=change.reason.value,
            target=(
                change.rendered.target.sql
                if change.rendered is not None
                else change.observed.qualified_name.sql
                if change.observed is not None
                else None
            ),
            fingerprint=change.rendered.fingerprint if change.rendered else None,
            previous_marker=(
                change.observed.marker.text
                if change.observed and change.observed.marker
                else (
                    content_hash(
                        {
                            "resources": [
                                (resource.object_type, resource.qualified_name.sql, resource.exists)
                                for resource in change.composite_observation.resources
                            ],
                            "stage_exists": change.composite_observation.stage_exists,
                            "stage_file_format": change.composite_observation.stage_file_format,
                            "config_path": change.composite_observation.config_path,
                            "config_exists": change.composite_observation.config_exists,
                            "config_size": change.composite_observation.config_size,
                            "config_md5": change.composite_observation.config_md5,
                            # Only handlers that record details add the key, so
                            # an eval plan's hash is unchanged by its existence.
                            **(
                                {"details": [list(item) for item in change.composite_observation.details]}
                                if change.composite_observation.details
                                else {}
                            ),
                        }
                    )
                    if change.composite_observation is not None
                    else None
                )
            ),
            statement_hashes=(
                tuple(sha256(str(statement).encode("utf-8")).hexdigest() for statement in change.rendered.statements)
                if change.rendered is not None
                else ()
            ),
            depends_on=change.depends_on,
            order=change.order,
            component_fingerprints=(
                tuple(sorted(change.rendered.component_fingerprints)) if change.rendered is not None else ()
            ),
            physical_resources=(
                tuple((object_type, name.sql) for object_type, name in change.rendered.physical_resources)
                if change.rendered is not None
                else ()
            ),
            prune_executable=change.prune_executable,
        )

    @classmethod
    def from_dict(cls, value: object) -> SavedChange:
        """Read one change of a saved plan, as `as_dict` wrote it.

        Raises:
            ValueError: the change, its component fingerprints, or its resources are malformed.
            KeyError: the change lacks a required key, such as `key` or `order`.
        """
        if not isinstance(value, dict):
            raise ValueError("saved plan change must be an object")
        return cls(
            key=str(value["key"]),
            artifact_type=str(value["artifact_type"]),
            action=str(value["action"]),
            reason=str(value["reason"]),
            target=_optional_text(value.get("target")),
            fingerprint=_optional_text(value.get("fingerprint")),
            previous_marker=_optional_text(value.get("previous_marker")),
            statement_hashes=tuple(str(element) for element in value.get("statement_hashes", [])),
            depends_on=tuple(str(element) for element in value.get("depends_on", [])),
            order=int(value["order"]),
            component_fingerprints=pairs_from_json(
                optional_object(
                    value.get("component_fingerprints"), "saved plan component_fingerprints must be an object"
                )
            ),
            physical_resources=resources_from_json(_saved_resources(value.get("physical_resources")), _RESOURCES),
            prune_executable=bool(value.get("prune_executable", True)),
        )

    def as_dict(self) -> dict[str, object]:
        """Return the change as the saved plan document stores it under `changes`."""
        return {
            "key": self.key,
            "artifact_type": self.artifact_type,
            "action": self.action,
            "reason": self.reason,
            "target": self.target,
            "fingerprint": self.fingerprint,
            "previous_marker": self.previous_marker,
            "statement_hashes": list(self.statement_hashes),
            "depends_on": list(self.depends_on),
            "order": self.order,
            "component_fingerprints": pairs_to_json(self.component_fingerprints),
            "physical_resources": resources_to_json(self.physical_resources),
            "prune_executable": self.prune_executable,
        }


@dataclass(frozen=True, slots=True)
class SavedPlan:
    """A plan saved for a later apply, identified by the hash of its document.

    Attributes:
        observation_at: When the plan observed Snowflake, as the observation recorded it.
        observation_fingerprint: The hash of each change's observed marker, target, and
            composite metadata; apply refuses the plan when a fresh plan's differs.
        selected, excluded, include_prune, partial: The selection the plan was made with, which
            apply repeats; `partial` is written only when set.
    """

    schema_version: int
    plan_id: str
    manifest_id: str
    target: TargetIdentity
    observation_at: str
    observation_fingerprint: str
    changes: tuple[SavedChange, ...]
    selected: tuple[str, ...] = ()
    excluded: tuple[str, ...] = ()
    include_prune: bool = False
    partial: bool = False

    @classmethod
    def from_changeset(
        cls,
        changeset: ChangeSet,
        *,
        selected: tuple[str, ...] = (),
        excluded: tuple[str, ...] = (),
        include_prune: bool = False,
        partial: bool = False,
    ) -> SavedPlan:
        """Save a changeset: fingerprint what it observed, then hash the whole document into the id."""
        changes = tuple(SavedChange.from_change(change) for change in changeset.changes)
        observed = [
            {
                "key": change.key,
                "marker": change.previous_marker,
                "target": change.target,
                "components": dict(change.component_fingerprints),
                "resources": list(change.physical_resources),
            }
            for change in changes
        ]
        observation_fingerprint = content_hash(observed)
        body = {
            "schema_version": PLAN_SCHEMA_VERSION,
            "manifest_id": changeset.manifest_id,
            "target": changeset.target.as_dict(),
            "observation_at": changeset.observation_at,
            "observation_fingerprint": observation_fingerprint,
            "changes": [change.as_dict() for change in changes],
            "selection": _selection(selected, excluded, include_prune, partial),
        }
        return cls(
            PLAN_SCHEMA_VERSION,
            content_hash(body),
            changeset.manifest_id,
            changeset.target,
            changeset.observation_at,
            observation_fingerprint,
            changes,
            selected,
            excluded,
            include_prune,
            partial,
        )

    def as_dict(self) -> dict[str, object]:
        """Return the saved plan document, which `from_dict` reads back."""
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "manifest_id": self.manifest_id,
            "target": self.target.as_dict(),
            "observation_at": self.observation_at,
            "observation_fingerprint": self.observation_fingerprint,
            "changes": [change.as_dict() for change in self.changes],
            "selection": _selection(self.selected, self.excluded, self.include_prune, self.partial),
        }

    def matches(self, manifest_id: str, target: TargetIdentity) -> bool:
        """Report whether the plan was made for this manifest and this target."""
        return self.manifest_id == manifest_id and self.target.key == target.key

    def check_applicable(self, current: SavedPlan, *, source: str) -> PlanMismatch | None:
        """Return why this saved plan cannot be applied in place of `current`; None when it can.

        `current` is a fresh plan of the same selection, made just now from the compiled
        project and the live target. The checks run in this order, and the first that fails is
        the one returned:

        1. The saved plan was made for the fresh plan's target.
        2. It was made from the fresh plan's manifest.
        3. What it observed in Snowflake is what the fresh plan observes.
        4. Its ordered changes, every statement hash included, are the fresh plan's: every
           recorded field of every change, in order.

        Args:
            source: How a report names the saved plan, such as its file path.

        Diagnostics:
            SST-APL005: the saved plan was made for another target; the mismatch carries it.
        """
        if self.target.key != current.target.key:
            diagnostic = D(
                "SST-APL005",
                artifact=source,
                found=_target_label(self.target),
                expected=_target_label(current.target),
            )
            return PlanMismatch(diagnostic.message, (diagnostic,))
        if not self.matches(current.manifest_id, current.target):
            return PlanMismatch("the saved plan was made from a different manifest; re-run sst plan")
        if self.observation_fingerprint != current.observation_fingerprint:
            return PlanMismatch("saved plan is stale; the live Snowflake observation changed")
        if self.changes != current.changes:
            return PlanMismatch("saved plan is stale; the ordered changes or statement hashes changed")
        return None

    @classmethod
    def from_dict(cls, value: object) -> SavedPlan:
        """Read a saved plan written by `as_dict`, refusing one whose recorded id is not its content hash.

        Raises:
            ValueError: the document is not a saved plan of this schema, or its id does not match.
            KeyError: a change lacks a required key.
            TypeError: a value has the wrong type to convert.
        """
        if not isinstance(value, dict):
            raise ValueError("saved plan must be an object")
        version = value.get("schema_version")
        if version != PLAN_SCHEMA_VERSION:
            raise ValueError(f"saved plan schema {version} is not supported")
        raw_changes = value.get("changes")
        if not isinstance(raw_changes, list):
            raise ValueError("saved plan changes must be a list")
        changes = tuple(SavedChange.from_dict(item) for item in raw_changes)
        raw_selection = value.get("selection", {})
        if not isinstance(raw_selection, dict):
            raise ValueError("saved plan selection must be an object")
        plan = cls(
            schema_version=version,
            plan_id=str(value.get("plan_id") or ""),
            manifest_id=str(value.get("manifest_id") or ""),
            target=TargetIdentity.from_dict(value.get("target")),
            observation_at=str(value.get("observation_at") or ""),
            observation_fingerprint=str(value.get("observation_fingerprint") or ""),
            changes=changes,
            selected=tuple(str(item) for item in raw_selection.get("selected", [])),
            excluded=tuple(str(item) for item in raw_selection.get("excluded", [])),
            include_prune=bool(raw_selection.get("include_prune", False)),
            partial=bool(raw_selection.get("partial", False)),
        )
        expected_body = dict(value)
        expected_body.pop("plan_id", None)
        expected = content_hash(expected_body)
        if plan.plan_id != expected:
            raise ValueError(f"saved plan id {plan.plan_id}, recomputed {expected}")
        return plan


def _selection(
    selected: tuple[str, ...], excluded: tuple[str, ...], include_prune: bool, partial: bool
) -> dict[str, object]:
    # `partial` appears only when set, so a plan that is not partial keeps its id.
    value: dict[str, object] = {"selected": list(selected), "excluded": list(excluded), "include_prune": include_prune}
    if partial:
        value["partial"] = True
    return value


@dataclass(frozen=True, slots=True)
class PlanMismatch:
    """Why a saved plan cannot be applied: what to report, and the diagnostic that names it.

    Attributes:
        message: The text to report the refusal with.
        diagnostics: The diagnostic behind the message; empty when the refusal has no code.
    """

    message: str
    diagnostics: tuple[Diagnostic, ...] = ()


def _target_label(target: TargetIdentity) -> str:
    """Name a target by its identity key, leaving out the parts it does not set."""
    return "/".join(part for part in target.key if part)


_RESOURCES = "saved plan physical_resources must be objects"


def _saved_resources(value: object) -> list[object]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(_RESOURCES)
    return value


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)
