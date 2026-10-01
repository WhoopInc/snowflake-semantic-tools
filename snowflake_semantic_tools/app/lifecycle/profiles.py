"""Publish Desktop profiles: verified content-addressed trees, then one guarded MERGE.

Each tree is uploaded under a prefix named by its own digest, so no upload can
change what a live row already points at. The MERGE on CONFIG_NAME is the atomic
switch, guarded by the VERSION the plan observed; afterwards the row is re-read
with Desktop's own query and every pointer must resolve. Profiles publish one at
a time. Nothing is ever deleted: prune deactivates a row, and only under --prune.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType
from typing import Mapping

from ...domain.model.diagnostic import D
from ...domain.model.identifier import QualifiedName
from ...domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ChangeReason,
    ClassifiedError,
    CompositeObservation,
    CompositePlan,
    OutcomeStatus,
    PhysicalResource,
    RenderedArtifact,
)
from ...domain.model.skill import BundleEntry
from ...domain.ports.snowflake import SnowflakePort, SnowflakePortError
from ...domain.state import DEACTIVATED, FAILED_AFTER_WRITE, AppliedEntry
from ..apply import classify_error
from ..compile.profiles import CompiledProfile, DesktopChannel
from ..desktop_contract import desktop_view, stage_pointers
from .composite import (
    SSE_STAGE_TYPE,
    CompositeHandler,
    PublicationRun,
    blocked,
    create_sse_stage_sql,
    details_stale,
    failed,
    skipped,
)

# The columns SST writes and the type family each must have.
REQUIRED_COLUMNS: Mapping[str, str] = MappingProxyType(
    {
        "CONFIG_NAME": "VARCHAR",
        "DESCRIPTION": "VARCHAR",
        "OWNER_TEAM": "VARCHAR",
        "VERSION": "VARCHAR",
        "ACTIVE": "BOOLEAN",
        "UPDATED_AT": "TIMESTAMP_NTZ",
        **{
            column: "VARIANT"
            for column in (
                "SKILL_REPOS",
                "MCP_SERVERS",
                "COMMAND_REPOS",
                "SYSTEM_PROMPT_REPO",
                "HOOKS",
                "PLUGINS",
                "ENV_VARS",
                "SETTINGS_OVERRIDES",
            )
        },
    }
)


@dataclass(frozen=True, slots=True)
class _Observed:
    stage_type: str | None
    columns: tuple[tuple[str, str], ...] | None
    row_exists: bool
    row_version: str | None
    row_active: bool
    trees: Mapping[str, tuple[str, ...]]


class ProfileLifecycleHandler(CompositeHandler[CompiledProfile, _Observed]):
    """Publish each Desktop profile's trees and registry row; a prune deactivates the row.

    Profiles publish and deactivate one at a time: every profile shares the registry
    and the shared tree.
    """

    artifact_type = "profile"
    _prune_order = 270

    def __init__(self, port: SnowflakePort, releases: Mapping[str, CompiledProfile]) -> None:
        super().__init__(port)
        self._releases = MappingProxyType(dict(releases))
        self._lock = Lock()

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        """Deactivation is the only removal, guarded by the VERSION SST last wrote."""
        recorded = dict(state_entry.component_fingerprints).get("version", "")
        observation = CompositeObservation(
            artifact_key,
            details=(
                ("config_name", artifact_key.split(":", 1)[1]),
                ("registry", state_entry.qualified_name),
                ("version", recorded),
            ),
        )
        return Change(
            artifact_key,
            self.artifact_type,
            Action.PRUNE,
            ChangeReason.ORPHANED,
            None,
            None,
            (),
            self._prune_order,
            composite_observation=observation,
            prune_executable=True,
        )

    def _subject(self, artifact: RenderedArtifact) -> CompiledProfile:
        return self._releases[artifact.key]

    def _observe(self, compiled: CompiledProfile) -> _Observed:
        channel = compiled.channel
        stage_type = self._port.stage_type(channel.stage)
        columns = self._port.table_columns(channel.registry)
        row = self._port.read_profile_row(channel.registry, compiled.name) if columns is not None else None
        trees = {
            tree.prefix: tuple(sorted(self._port.list_location(f"@{channel.stage.sql}/{tree.prefix}")))
            for tree in compiled.release.trees
            if stage_type is not None
        }
        view = desktop_view(row) if row is not None else {}
        version = view.get("VERSION")
        active = row.get("ACTIVE") if row is not None else None
        return _Observed(
            stage_type=stage_type,
            columns=columns,
            row_exists=row is not None,
            row_version=str(version) if row is not None and version is not None else None,
            row_active=active is True or str(active).casefold() == "true",
            trees=MappingProxyType(trees),
        )

    def _decide(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        compiled: CompiledProfile,
        observed: _Observed,
    ) -> CompositePlan:
        observation = self._observation(compiled, observed)
        refusal = _refusal(artifact.key, state_entry, compiled, observed, observation)
        if refusal is not None:
            return refusal
        action, reason = _action(artifact, state_entry, compiled, observed)
        return CompositePlan(action, reason, observation)

    def _publish(self, change: Change, artifact: RenderedArtifact) -> ApplyOutcome:
        compiled = self._releases[change.key]
        # One profile at a time: every profile shares the registry and the shared tree.
        with self._lock:
            current = self._observe(compiled)
            planned = change.composite_observation
            if planned is None or _stale(planned.details, self._observation(compiled, current).details):
                registry = compiled.channel.registry.sql
                return failed(change, f"{registry} row '{compiled.name}' changed since the plan", code="SST-APL012")
            return _ProfileRun(self._port, change, artifact, compiled, current).publish()

    def _apply_prune(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Deactivate the row under --prune, only while it carries the VERSION state recorded."""
        observation = change.composite_observation
        if not options.allow_prune or observation is None:
            return skipped(change)
        details = dict(observation.details)
        registry = QualifiedName.parse(details["registry"])
        if not details.get("version"):
            return _refused(
                change, f"state records no VERSION for row '{details['config_name']}'; refusing to deactivate it", 0
            )
        with self._lock:
            changed = self._port.deactivate_profile_row(
                registry, details["config_name"], expected_version=details["version"]
            )
        if changed != 1:
            if self._port.read_profile_row(registry, details["config_name"]) is None:
                # Never inserted, or removed by hand: there is nothing left to retire.
                return ApplyOutcome(change.key, change.action, OutcomeStatus.APPLIED, 1, 0, "")
            return _refused(
                change,
                f"{registry.sql} row '{details['config_name']}' no longer carries VERSION {details['version']}",
                1,
            )
        return ApplyOutcome(change.key, change.action, OutcomeStatus.APPLIED, 1, 0, "", write_succeeded=True)

    def _observation(self, compiled: CompiledProfile, observed: _Observed) -> CompositeObservation:
        return CompositeObservation(
            key=compiled.artifact_key,
            resources=(
                PhysicalResource("STAGE", compiled.channel.stage, observed.stage_type is not None),
                PhysicalResource("TABLE", compiled.channel.registry, observed.columns is not None),
            ),
            stage_exists=observed.stage_type is not None,
            details=(
                ("stage_type", observed.stage_type or ""),
                ("registry", "present" if observed.columns is not None else "absent"),
                ("row_version", observed.row_version or ""),
                ("row_active", "true" if observed.row_active else "false"),
                ("complete_trees", ",".join(prefix for prefix, paths in sorted(observed.trees.items()) if paths)),
            ),
        )


class _ProfileRun(PublicationRun):
    """One profile publication: registry, stage, and trees, then the guarded MERGE and its read-back."""

    def __init__(
        self,
        port: SnowflakePort,
        change: Change,
        artifact: RenderedArtifact,
        compiled: CompiledProfile,
        current: _Observed,
    ) -> None:
        super().__init__(port, change, artifact)
        self._compiled = compiled
        self._current = current
        # `_written` counts any write at all, trees included. Trees are content-addressed,
        # so they change nothing Desktop reads; only the row does, and state follows it.
        self._row_written = False

    def publish(self) -> ApplyOutcome:
        """Create what is missing and upload the trees, then MERGE the row and read it back as Desktop does."""
        try:
            failure = self._run_steps(self._ensure_registry, self._ensure_stage, self._upload_trees)
            if failure is not None:
                return failure
            self._merge_row()
            return self._verify()
        except SnowflakePortError as exc:
            return self._interrupted(exc)

    def _owns_write(self) -> bool:
        # State records a failed publish only once the row carries it: recording a
        # VERSION the row never got would read as another writer on the next plan.
        return self._row_written

    def _recorded_resources(self) -> tuple[tuple[str, str], ...]:
        return _resources(self._compiled.channel) if self._row_written else ()

    def _interrupted(self, error: SnowflakePortError) -> ApplyOutcome:
        """Fail on a port error, as a partial write once anything was written (SST-APL018)."""
        return self.fail(f"{error}", "SST-APL018" if self._written else "SST-APL001")

    def _ensure_registry(self) -> ApplyOutcome | None:
        if self._current.columns is not None:
            return None
        registry = self._compiled.channel.registry
        self._attempts += 1
        self._port.ensure_profile_registry(registry)
        self._written = True
        shape = _shape_problem(self._port.table_columns(registry))
        if shape is not None:
            return self.fail(f"{registry.sql} {shape} after creation", "SST-APL016")
        return None

    def _ensure_stage(self) -> ApplyOutcome | None:
        if self._current.stage_type is not None:
            return None
        result = self._port.execute_script((create_sse_stage_sql(self._compiled.channel.stage),))
        self._attempts += 1
        if not result.ok:
            return self.fail(result.error.message if result.error else "stage creation failed")
        self._written = True
        return None

    def _upload_trees(self) -> ApplyOutcome | None:
        stage = self._compiled.channel.stage
        for tree in self._compiled.release.trees:
            failure = self._upload_tree(f"@{stage.sql}/{tree.prefix}", tree.entries)
            if failure is not None:
                return failure
        return None

    def _upload_tree(self, prefix: str, bundle: tuple[BundleEntry, ...]) -> ApplyOutcome | None:
        failed_upload = self._upload_missing(prefix, bundle, set(self._port.list_location(prefix)))
        if failed_upload is not None:
            return self._interrupted(failed_upload[1])
        return self._verify_staged(
            prefix, bundle, lambda listed: f"{prefix} holds {len(listed)} files, expected {len(bundle)}"
        )

    def _merge_row(self) -> None:
        """MERGE the release row, guarded by the VERSION the plan saw, and record whether it landed."""
        compiled = self._compiled
        self._attempts += 1
        try:
            changed = self._port.merge_profile_row(
                compiled.channel.registry,
                compiled.release.row,
                expected_version=self._current.row_version,
            )
        except SnowflakePortError:
            # The MERGE may have committed before its error reached us. If the
            # re-read fails too, the outcome is unknown; recording is safe only
            # when no row existed: an uncommitted MERGE then plans CREATE, and a
            # rival's row reads as SST-PLN028 rather than as ours.
            try:
                self._row_written = self._row_carries_release()
            except SnowflakePortError:
                self._row_written = not self._current.row_exists
            raise
        self._row_written = changed > 0
        self._written = self._written or self._row_written

    def _verify(self) -> ApplyOutcome:
        """Read the row with Desktop's own query: it must carry this VERSION and every pointer resolve."""
        compiled = self._compiled
        channel = compiled.channel
        rows = self._port.desktop_profile_rows(channel.registry)
        row = next((item for item in rows if str(desktop_view(item).get("CONFIG_NAME")) == compiled.name), None)
        if row is None:
            return self.fail(f"Desktop's query returns no active row '{compiled.name}'", "SST-APL012")
        view = desktop_view(row)
        if str(view.get("VERSION")) != compiled.release.version:
            return self.fail(
                f"row '{compiled.name}' carries VERSION {view.get('VERSION')}, another writer won the MERGE",
                "SST-APL012",
            )
        for pointer in stage_pointers(view):
            if not self._resolves(pointer):
                return self.fail(f"row '{compiled.name}' points at {pointer}, which does not resolve", "SST-APL016")
        return self.applied(
            write_succeeded=True,
            component_fingerprints=self._artifact.component_fingerprints,
            physical_resources=_resources(channel),
        )

    def _resolves(self, pointer: str) -> bool:
        if pointer.endswith("/"):
            return bool(self._port.list_location(pointer))
        directory, _, name = pointer.rpartition("/")
        return name in self._port.list_location(f"{directory}/")

    def _row_carries_release(self) -> bool:
        row = self._port.read_profile_row(self._compiled.channel.registry, self._compiled.name)
        return row is not None and str(desktop_view(row).get("VERSION")) == self._compiled.release.version


def _refusal(
    key: str,
    state_entry: AppliedEntry | None,
    compiled: CompiledProfile,
    observed: _Observed,
    observation: CompositeObservation,
) -> CompositePlan | None:
    """Block the plan when the stage, registry, or row is not one SST may write.

    Diagnostics:
        SST-PLN026: the profile stage encrypts client-side.
        SST-PLN029: the registry lacks a column SST writes, or has it with another type.
        SST-PLN024: the row exists, but state does not record it as SST's.
        SST-PLN028: the row carries a VERSION neither state nor this release names.
    """
    stage_type = observed.stage_type
    if stage_type and stage_type.upper() != SSE_STAGE_TYPE:
        return blocked(
            observation,
            D("SST-PLN026", subject=key, artifact=key, value=compiled.channel.stage.sql, found=stage_type),
        )
    shape = _shape_problem(observed.columns)
    if shape is not None:
        return blocked(
            observation,
            D("SST-PLN029", subject=key, artifact=key, value=compiled.channel.registry.sql, detail=shape),
        )
    if not observed.row_exists:
        return None
    if state_entry is None:
        return blocked(
            observation,
            D("SST-PLN024", subject=key, artifact=key, value=f"{compiled.channel.registry.sql} row '{compiled.name}'"),
            ChangeReason.UNMANAGED_OBJECT,
        )
    recorded = dict(state_entry.component_fingerprints).get("version")
    if observed.row_version not in (recorded, compiled.release.version):
        return blocked(
            observation,
            D(
                "SST-PLN028",
                subject=key,
                artifact=key,
                value=compiled.name,
                found=observed.row_version or "NULL",
                expected=recorded or "nothing",
            ),
        )
    return None


def _action(
    artifact: RenderedArtifact,
    state_entry: AppliedEntry | None,
    compiled: CompiledProfile,
    observed: _Observed,
) -> tuple[Action, ChangeReason]:
    """Decide what an unblocked profile needs: its row, its trees, or only a state record."""
    desired = compiled.release.version
    # A `deactivated` entry is the tombstone SST's own --prune left: the row is
    # still SST's, carrying the VERSION it deactivated, and can be reactivated.
    retired = state_entry is not None and state_entry.outcome == DEACTIVATED
    complete = all(observed.trees.get(tree.prefix) == tuple(sorted(tree.paths)) for tree in compiled.release.trees)
    if not observed.row_exists:
        return Action.CREATE, ChangeReason.NOT_PRESENT
    if observed.row_version != desired or not observed.row_active or not complete:
        return Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS
    if state_entry is not None and (
        state_entry.fingerprint != artifact.fingerprint or retired or state_entry.outcome == FAILED_AFTER_WRITE
    ):
        return Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH
    return Action.NOOP, ChangeReason.UNCHANGED


def _refused(change: Change, detail: str, attempts: int) -> ApplyOutcome:
    """Fail a deactivation SST cannot vouch for (SST-APL012); retrying the statement cannot help."""
    error = ClassifiedError("SST-APL012", detail, classify_error(detail).kind)
    return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, attempts, 0, "", error)


def _resources(channel: DesktopChannel) -> tuple[tuple[str, str], ...]:
    return ("STAGE", channel.stage.sql), ("TABLE", channel.registry.sql)


def _shape_problem(columns: tuple[tuple[str, str], ...] | None) -> str | None:
    if columns is None:
        return None
    found = {str(name).upper(): str(kind).upper() for name, kind in columns}
    missing = sorted(set(REQUIRED_COLUMNS) - set(found))
    if missing:
        return f"lacks {', '.join(missing)}"
    wrong = sorted(name for name, family in REQUIRED_COLUMNS.items() if not found[name].startswith(family))
    return f"types {', '.join(f'{name} {found[name]}' for name in wrong)} differ" if wrong else None


def _stale(planned: tuple[tuple[str, str], ...], current: tuple[tuple[str, str], ...]) -> bool:
    """Only the row and the containers matter: trees are content-addressed and re-verified."""
    return details_stale(
        planned,
        current,
        reverified=frozenset(("complete_trees",)),
        creatable={"stage_type": ("", SSE_STAGE_TYPE), "registry": ("absent", "present")},
    )
