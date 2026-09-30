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

from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import (
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
from ..domain.model.skill import BundleEntry
from ..domain.ports.snowflake import SnowflakePort, SnowflakePortError
from ..domain.state import DEACTIVATED, FAILED_AFTER_WRITE, AppliedEntry, AppliedResourceInput, Manifest
from .apply import classify_error
from .desktop_contract import desktop_view, stage_pointers
from .extension_lifecycle import SSE_STAGE_TYPE
from .profile_compile import CompiledProfile

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


class ProfileLifecycleHandler:
    artifact_type = "profile"

    def __init__(self, port: SnowflakePort, releases: Mapping[str, CompiledProfile]) -> None:
        self._port = port
        self._releases = MappingProxyType(dict(releases))
        self._lock = Lock()

    def plan(self, artifact: RenderedArtifact, state_entry: AppliedEntry | None, manifest: Manifest) -> CompositePlan:
        del manifest
        compiled = self._releases[artifact.key]
        try:
            observed = self._observe(compiled)
        except SnowflakePortError as exc:
            diagnostic = D("SST-PLN001", value=artifact.key, detail=str(exc))
            empty = CompositeObservation(artifact.key, diagnostics=DiagnosticBag((diagnostic,)))
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, empty, empty.diagnostics)
        observation = self._observation(compiled, observed)
        stage_type = observed.stage_type
        if stage_type and stage_type.upper() != SSE_STAGE_TYPE:
            return _blocked(
                observation,
                D(
                    "SST-PLN026",
                    subject=artifact.key,
                    artifact=artifact.key,
                    value=compiled.channel.stage.sql,
                    found=stage_type,
                ),
            )
        shape = _shape_problem(observed.columns)
        if shape is not None:
            return _blocked(
                observation,
                D(
                    "SST-PLN029",
                    subject=artifact.key,
                    artifact=artifact.key,
                    value=compiled.channel.registry.sql,
                    detail=shape,
                ),
            )
        desired = compiled.release.version
        row_version = observed.row_version
        # A `deactivated` entry is the tombstone SST's own --prune left: the row is
        # still SST's, carrying the VERSION it deactivated, and can be reactivated.
        retired = state_entry is not None and state_entry.outcome == DEACTIVATED
        if observed.row_exists:
            if state_entry is None:
                return _blocked(
                    observation,
                    D(
                        "SST-PLN024",
                        subject=artifact.key,
                        artifact=artifact.key,
                        value=f"{compiled.channel.registry.sql} row '{compiled.name}'",
                    ),
                    reason=ChangeReason.UNMANAGED_OBJECT,
                )
            recorded = dict(state_entry.component_fingerprints).get("version")
            if row_version not in (recorded, desired):
                return _blocked(
                    observation,
                    D(
                        "SST-PLN028",
                        subject=artifact.key,
                        artifact=artifact.key,
                        value=compiled.name,
                        found=row_version or "NULL",
                        expected=recorded or "nothing",
                    ),
                )
        complete = all(observed.trees.get(tree.prefix) == tuple(sorted(tree.paths)) for tree in compiled.release.trees)
        if not observed.row_exists:
            action, reason = Action.CREATE, ChangeReason.NOT_PRESENT
        elif row_version != desired or not observed.row_active or not complete:
            action, reason = Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS
        elif state_entry is not None and (
            state_entry.fingerprint != artifact.fingerprint or retired or state_entry.outcome == FAILED_AFTER_WRITE
        ):
            action, reason = Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH
        else:
            action, reason = Action.NOOP, ChangeReason.UNCHANGED
        return CompositePlan(action, reason, observation)

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        if change.action is Action.PRUNE:
            return self._deactivate(change, options)
        artifact = change.rendered
        if change.action in (Action.NOOP, Action.BLOCKED) or artifact is None:
            return ApplyOutcome(
                change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, artifact.ddl if artifact else ""
            )
        compiled = self._releases[change.key]
        # One profile at a time: every profile shares the registry and the shared tree.
        with self._lock:
            return _ProfileRun(self, change, compiled).publish()

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
            270,
            composite_observation=observation,
            prune_executable=True,
        )

    @staticmethod
    def merge_physical_resources(
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[AppliedResourceInput, ...]:
        del previous
        return current

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

    def _deactivate(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        observation = change.composite_observation
        if not options.allow_prune or observation is None:
            return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, "")
        details = dict(observation.details)
        registry = QualifiedName.parse(details["registry"])
        if not details.get("version"):
            detail = f"state records no VERSION for row '{details['config_name']}'; refusing to deactivate it"
            error = ClassifiedError("SST-APL012", detail, classify_error(detail).kind)
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, "", error)
        with self._lock:
            changed = self._port.deactivate_profile_row(
                registry, details["config_name"], expected_version=details["version"]
            )
        if changed != 1:
            if self._port.read_profile_row(registry, details["config_name"]) is None:
                # Never inserted, or removed by hand: there is nothing left to retire.
                return ApplyOutcome(change.key, change.action, OutcomeStatus.APPLIED, 1, 0, "")
            detail = f"{registry.sql} row '{details['config_name']}' no longer carries VERSION {details['version']}"
            error = ClassifiedError("SST-APL012", detail, classify_error(detail).kind)
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 1, 0, "", error)
        return ApplyOutcome(change.key, change.action, OutcomeStatus.APPLIED, 1, 0, "", write_succeeded=True)


class _ProfileRun:
    def __init__(self, handler: ProfileLifecycleHandler, change: Change, compiled: CompiledProfile) -> None:
        self._handler = handler
        self._port = handler._port
        self._change = change
        self._compiled = compiled
        self._attempts = 0
        # Any write at all, trees included. Trees are content-addressed, so they
        # change nothing Desktop reads; only the row does, and state follows it.
        self._written = False
        self._row_written = False

    def publish(self) -> ApplyOutcome:
        compiled = self._compiled
        channel = compiled.channel
        current = self._handler._observe(compiled)
        planned = self._change.composite_observation
        if planned is None or _stale(planned.details, self._handler._observation(compiled, current).details):
            return self._fail(f"{channel.registry.sql} row '{compiled.name}' changed since the plan", "SST-APL012")
        try:
            if current.columns is None:
                self._attempts += 1
                self._port.ensure_profile_registry(channel.registry)
                self._written = True
                shape = _shape_problem(self._port.table_columns(channel.registry))
                if shape is not None:
                    return self._fail(f"{channel.registry.sql} {shape} after creation", "SST-APL016")
            if current.stage_type is None:
                result = self._port.execute_script(
                    (f"CREATE STAGE IF NOT EXISTS {channel.stage.sql} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')",)
                )
                self._attempts += 1
                if not result.ok:
                    return self._fail(result.error.message if result.error else "stage creation failed")
                self._written = True
            for tree in compiled.release.trees:
                failure = self._upload_tree(f"@{channel.stage.sql}/{tree.prefix}", tree.entries)
                if failure is not None:
                    return failure
            self._attempts += 1
            try:
                changed = self._port.merge_profile_row(
                    channel.registry,
                    compiled.release.row,
                    expected_version=current.row_version,
                )
            except SnowflakePortError:
                # The MERGE may have committed before its error reached us. If the
                # re-read fails too, the outcome is unknown; recording is safe only
                # when no row existed: an uncommitted MERGE then plans CREATE, and a
                # rival's row reads as SST-PLN028 rather than as ours.
                try:
                    self._row_written = self._row_carries_release()
                except SnowflakePortError:
                    self._row_written = not current.row_exists
                raise
            self._row_written = changed > 0
            self._written = self._written or self._row_written
            return self._verify()
        except SnowflakePortError as exc:
            code = "SST-APL018" if self._written else "SST-APL001"
            return self._fail(f"{exc}", code)

    def _upload_tree(self, prefix: str, bundle: tuple[BundleEntry, ...]) -> ApplyOutcome | None:
        present = set(self._port.list_location(prefix))
        for entry in bundle:
            if entry.path in present:
                continue
            self._attempts += 1
            self._port.upload(f"{prefix}{entry.path}", entry.content)
            self._written = True
        listed = tuple(sorted(self._port.list_location(prefix)))
        expected = tuple(sorted(entry.path for entry in bundle))
        if listed != expected:
            return self._fail(f"{prefix} holds {len(listed)} files, expected {len(expected)}", "SST-APL016")
        for entry in bundle:
            if self._port.read_staged_file(f"{prefix}{entry.path}") != entry.content:
                return self._fail(f"{prefix}{entry.path} does not read back byte for byte", "SST-APL016")
        return None

    def _verify(self) -> ApplyOutcome:
        compiled = self._compiled
        channel = compiled.channel
        rows = self._port.desktop_profile_rows(channel.registry)
        row = next((item for item in rows if str(desktop_view(item).get("CONFIG_NAME")) == compiled.name), None)
        if row is None:
            return self._fail(f"Desktop's query returns no active row '{compiled.name}'", "SST-APL012")
        view = desktop_view(row)
        if str(view.get("VERSION")) != compiled.release.version:
            return self._fail(
                f"row '{compiled.name}' carries VERSION {view.get('VERSION')}, another writer won the MERGE",
                "SST-APL012",
            )
        for pointer in stage_pointers(view):
            if not self._resolves(pointer):
                return self._fail(f"row '{compiled.name}' points at {pointer}, which does not resolve", "SST-APL016")
        artifact = self._change.rendered
        assert artifact is not None
        return ApplyOutcome(
            self._change.key,
            self._change.action,
            OutcomeStatus.APPLIED,
            max(self._attempts, 1),
            0,
            artifact.ddl,
            write_succeeded=True,
            component_fingerprints=artifact.component_fingerprints,
            physical_resources=(("STAGE", channel.stage.sql), ("TABLE", channel.registry.sql)),
        )

    def _resolves(self, pointer: str) -> bool:
        if pointer.endswith("/"):
            return bool(self._port.list_location(pointer))
        directory, _, name = pointer.rpartition("/")
        return name in self._port.list_location(f"{directory}/")

    def _row_carries_release(self) -> bool:
        row = self._port.read_profile_row(self._compiled.channel.registry, self._compiled.name)
        return row is not None and str(desktop_view(row).get("VERSION")) == self._compiled.release.version

    def _fail(self, detail: str, code: str = "SST-APL001") -> ApplyOutcome:
        artifact = self._change.rendered
        classified = classify_error(detail)
        channel = self._compiled.channel
        # State records a failed publish only once the row carries it: recording a
        # VERSION the row never got would read as another writer on the next plan.
        return ApplyOutcome(
            self._change.key,
            self._change.action,
            OutcomeStatus.FAILED,
            self._attempts,
            0,
            artifact.ddl if artifact is not None else "",
            ClassifiedError(code, detail, classified.kind, classified.retryable, classified.sqlstate),
            write_succeeded=self._row_written,
            component_fingerprints=artifact.component_fingerprints if artifact is not None else (),
            physical_resources=(
                (("STAGE", channel.stage.sql), ("TABLE", channel.registry.sql)) if self._row_written else ()
            ),
        )


def _blocked(
    observation: CompositeObservation,
    diagnostic: Diagnostic,
    *,
    reason: ChangeReason = ChangeReason.VALIDATION_ERRORS,
) -> CompositePlan:
    return CompositePlan(Action.BLOCKED, reason, observation, DiagnosticBag((diagnostic,)))


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
    before = {key: value for key, value in planned if key != "complete_trees"}
    after = {key: value for key, value in current if key != "complete_trees"}
    if before.get("stage_type") == "" and after.get("stage_type") == SSE_STAGE_TYPE:
        before["stage_type"] = SSE_STAGE_TYPE
    if before.get("registry") == "absent" and after.get("registry") == "present":
        before["registry"] = "present"
    return before != after
