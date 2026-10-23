"""Publish Desktop profiles: verified content-addressed trees, then one guarded MERGE.

Each tree is uploaded under a prefix named by its own digest, so no upload can
change what a live row already points at. The MERGE on CONFIG_NAME is the atomic
switch, guarded by the VERSION the plan observed; afterwards the row is re-read
with Desktop's own query and every pointer must resolve. Profiles publish one at
a time. Nothing is ever deleted: prune deactivates a row, and only under --prune.
Lost state is recovered from the row itself, when Desktop reads it as the release.

VERSION is a counter, as Desktop shows it: a new row is 1, and a row whose content
changes takes the live VERSION plus one. Whether the content changed is decided by
comparing the columns Desktop reads, VERSION aside; state records the VERSION written.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from hashlib import sha256
from threading import Lock
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompiledCocoDesktopProfile, DesktopChannel
from snowflake_semantic_tools.app.desktop_contract import desktop_view, stage_pointers
from snowflake_semantic_tools.app.lifecycle.composite import (
    CompositeHandler,
    PublicationRun,
    blocked,
    create_sse_stage_sql,
    details_stale,
    failed,
    skipped,
)
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.diagnostics.signatures import match_signature
from snowflake_semantic_tools.domain.model.coco_desktop_profile import is_counter, next_version, same_version
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ABSENT,
    BUNDLE_PREFIX_MISSING,
    OTHER_DEFINITION,
    RECORDED,
    ROW_DIFFERS,
    SSE_STAGE_TYPE,
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
    RecoveryProof,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.model.skill import BundleEntry
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.profile_registry import ProfileRegistryPort
from snowflake_semantic_tools.domain.state import DEACTIVATED, FAILED_AFTER_WRITE, AppliedEntry

# The columns SST writes and the type family each must have.
REQUIRED_COLUMNS: Mapping[str, str] = MappingProxyType(
    {
        "CONFIG_NAME": "VARCHAR",
        "DESCRIPTION": "VARCHAR",
        "OWNER_TEAM": "VARCHAR",
        "VERSION": "VARCHAR",
        "ACTIVE": "BOOLEAN",
        "UPDATED_AT": "TIMESTAMP_NTZ",
        **dict.fromkeys(
            (
                "SKILL_REPOS",
                "MCP_SERVERS",
                "COMMAND_REPOS",
                "SYSTEM_PROMPT_REPO",
                "HOOKS",
                "PLUGINS",
                "ENV_VARS",
                "SETTINGS_OVERRIDES",
            ),
            "VARIANT",
        ),
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
    row_content: str | None = None

    def version_for(self, content: str) -> str:
        """Return the VERSION a row with `content` is written with.

        The live VERSION when the row already holds that content as a counter, else the next
        counter: 1 for a new row.
        """
        if self.row_content == content and self.row_version is not None and is_counter(self.row_version):
            return self.row_version
        return next_version(self.row_version)


class CocoDesktopProfilePublicationPort(CatalogPublicationPort, ProfileRegistryPort, Protocol):
    """The roles publishing a profile uses: a catalog publication port that also keeps the registry."""


class CocoDesktopProfileLifecycleHandler(
    CompositeHandler[CompiledCocoDesktopProfile, _Observed, CocoDesktopProfilePublicationPort]
):
    """Publish each Desktop profile's trees and registry row; a prune deactivates the row.

    Profiles publish and deactivate one at a time: every profile shares the registry
    and the shared tree.
    """

    artifact_type = "coco_desktop_profile"

    def __init__(
        self, port: CocoDesktopProfilePublicationPort, releases: Mapping[str, CompiledCocoDesktopProfile]
    ) -> None:
        super().__init__(port)
        self._releases = MappingProxyType(dict(releases))
        self._lock = Lock()

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        """Deactivation is the only removal, guarded by the VERSION SST last wrote."""
        fingerprints = dict(state_entry.component_fingerprints)
        recorded = fingerprints.get("version", "")
        observation = CompositeObservation(
            artifact_key,
            details=(
                ("config_name", fingerprints.get("config_name") or artifact_key.split(":", 1)[1]),
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

    def _subject(self, artifact: RenderedArtifact) -> CompiledCocoDesktopProfile:
        return self._releases[artifact.key]

    def _observe(self, compiled: CompiledCocoDesktopProfile) -> _Observed:
        channel = compiled.channel
        stage_type = self._port.stage_type(channel.stage)
        columns = self._port.table_columns(channel.registry)
        row = self._port.read_profile_row(channel.registry, compiled.config_name) if columns is not None else None
        trees = {
            tree.prefix: tuple(sorted(self._port.list_location(f"@{channel.stage.sql}/{tree.prefix}")))
            for tree in compiled.release.trees
            if stage_type is not None
        }
        view = desktop_view(row) if row is not None else {}
        # The column's own text, not Desktop's JSON reading of it: the MERGE guard compares text.
        version = next((value for key, value in (row or {}).items() if str(key).upper() == "VERSION"), None)
        active = row.get("ACTIVE") if row is not None else None
        return _Observed(
            stage_type=stage_type,
            columns=columns,
            row_exists=row is not None,
            row_version=str(version) if row is not None and version is not None else None,
            row_active=active is True or str(active).casefold() == "true",
            trees=MappingProxyType(trees),
            row_content=row_content(view) if row is not None else None,
        )

    def _decide(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        compiled: CompiledCocoDesktopProfile,
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
                return failed(
                    change, f"{registry} row '{compiled.config_name}' changed since the plan", code="SST-APL012"
                )
            return _CocoDesktopProfileRun(self._port, change, artifact, compiled, current).publish()

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

    def recover(self, change: Change) -> RecoveryProof:
        """Prove an unrecorded profile's registry row is this release's, as Desktop reads it.

        The registry must have every column SST writes, with its type, and the row, read again,
        must show Desktop exactly the release's columns, VERSION aside, on a server-side
        encrypted stage; state records the VERSION it carries. ACTIVE is not compared: an
        inactive row is recorded, and the next apply reactivates it. The trees are content-addressed under the row's
        pointers, so they are not read: the next plan publishes any that is incomplete.

        Raises:
            SnowflakePortError: a read failed, or more than one row has the profile's name.
        """
        compiled = self._releases[change.key]
        channel = compiled.channel
        name = f"{channel.registry.sql} row '{compiled.config_name}'"
        columns = self._port.table_columns(channel.registry)
        if columns is None:
            return RecoveryProof(ABSENT, qualified_name=name)
        if _shape_problem(columns) is not None:
            return RecoveryProof(OTHER_DEFINITION, qualified_name=name)
        row = self._port.read_profile_row(channel.registry, compiled.config_name)
        if row is None:
            return RecoveryProof(ABSENT, qualified_name=name)
        view = desktop_view(row)
        if row_content(view) != release_content(compiled):
            return RecoveryProof(ROW_DIFFERS, qualified_name=name)
        stage_type = self._port.stage_type(channel.stage)
        if stage_type is None or stage_type.upper() != SSE_STAGE_TYPE:
            return RecoveryProof(OTHER_DEFINITION if stage_type else BUNDLE_PREFIX_MISSING, qualified_name=name)
        assert change.rendered is not None
        fingerprints = _with_version(change.rendered.component_fingerprints, str(view.get("VERSION") or ""))
        return RecoveryProof(RECORDED, fingerprints, _resources(channel), name)

    def _observation(self, compiled: CompiledCocoDesktopProfile, observed: _Observed) -> CompositeObservation:
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
                ("row_content", observed.row_content or ""),
                ("row_active", "true" if observed.row_active else "false"),
                ("version", observed.version_for(release_content(compiled))),
                ("complete_trees", ",".join(prefix for prefix, paths in sorted(observed.trees.items()) if paths)),
            ),
        )


class _CocoDesktopProfileRun(PublicationRun[CocoDesktopProfilePublicationPort]):
    """One profile publication: registry, stage, and trees, then the guarded MERGE and its read-back."""

    def __init__(
        self,
        port: CocoDesktopProfilePublicationPort,
        change: Change,
        artifact: RenderedArtifact,
        compiled: CompiledCocoDesktopProfile,
        current: _Observed,
    ) -> None:
        super().__init__(port, change, artifact)
        self._compiled = compiled
        self._current = current
        # `_written` counts any write at all, trees included. Trees are content-addressed,
        # so they change nothing Desktop reads; only the row does, and state follows it.
        self._row_written = False
        # Every tree is on the stage: a failure from here on is the registry channel's alone.
        self._uploaded = False
        self._content = release_content(compiled)
        self._version = current.version_for(self._content)

    def publish(self) -> ApplyOutcome:
        """Create what is missing and upload the trees, then MERGE the row and read it back as Desktop does."""
        try:
            failure = self._run_steps(self._ensure_registry, self._ensure_stage, self._upload_trees)
            if failure is not None:
                return failure
            self._uploaded = True
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

    def _fingerprints(self) -> tuple[tuple[str, str], ...]:
        """The release's fingerprints, with the VERSION this run writes, which state guards prune with."""
        return _with_version(self._artifact.component_fingerprints, self._version)

    def fail(self, detail: str, code: str = "SST-APL001", *, value: str = "") -> ApplyOutcome:
        """Fail as a publication does, recording the VERSION once the row carries it."""
        outcome = super().fail(detail, code, value=value)
        return replace(outcome, component_fingerprints=self._fingerprints()) if self._row_written else outcome

    def _interrupted(self, error: SnowflakePortError) -> ApplyOutcome:
        """Fail on a port error: the registry write after the upload (SST-APL018), else a partial write.

        A failure before every tree is uploaded is a partial write once anything was written
        (SST-APL016), and a plain failure otherwise.
        """
        if self._uploaded:
            return self.fail(f"{error}", "SST-APL018", value=f"@{self._compiled.channel.stage.sql}")
        return self.fail(f"{error}", "SST-APL016" if self._written else "SST-APL001")

    def _ensure_registry(self) -> ApplyOutcome | None:
        if self._current.columns is not None:
            return None
        registry = self._compiled.channel.registry
        self._attempts += 1
        self._port.ensure_profile_registry(registry)
        self._written = True
        columns = self._port.table_columns(registry)
        shape = "is absent" if columns is None else _shape_problem(columns)
        if shape is not None:
            return self.fail(f"the registry {shape} after creation", "SST-APL027", value=registry.sql)
        return None

    def _ensure_stage(self) -> ApplyOutcome | None:
        if self._current.stage_type is not None:
            return None
        result = self._run_statement(create_sse_stage_sql(self._compiled.channel.stage))
        if not result.ok:
            return self.fail(result.error.message if result.error else "stage creation failed")
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
        """MERGE the release row at its VERSION, guarded by the one the plan saw; record whether it landed."""
        compiled = self._compiled
        self._attempts += 1
        try:
            changed = self._port.merge_profile_row(
                compiled.channel.registry,
                {**compiled.release.row, "VERSION": self._version},
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
        """Read the row as Desktop does: it must carry this VERSION and content, and every pointer resolve."""
        compiled = self._compiled
        channel = compiled.channel
        name = compiled.config_name
        rows = self._port.desktop_profile_rows(channel.registry)
        row = next((item for item in rows if str(desktop_view(item).get("CONFIG_NAME")) == name), None)
        if row is None:
            return self.fail(f"Desktop's query returns no active row '{name}'", "SST-APL012")
        view = desktop_view(row)
        if str(view.get("VERSION")) != self._version:
            return self.fail(
                f"row '{name}' carries VERSION {view.get('VERSION')}, another writer won the MERGE",
                "SST-APL012",
            )
        for pointer in stage_pointers(view):
            if not self._resolves(pointer):
                # The registry channel reports success; the stage channel does not hold the tree.
                return self.fail(
                    f"the registry row '{name}' was written, but the stage does not hold {pointer}",
                    "SST-APL017",
                )
        if row_content(view) != self._content:
            # Two writers counted to the same VERSION; the content shows which one won.
            return self.fail(f"row '{name}' carries VERSION {self._version} with other content", "SST-APL012")
        return self.applied(
            write_succeeded=True,
            component_fingerprints=self._fingerprints(),
            physical_resources=_resources(channel),
        )

    def _resolves(self, pointer: str) -> bool:
        if pointer.endswith("/"):
            return bool(self._port.list_location(pointer))
        directory, _, name = pointer.rpartition("/")
        return name in self._port.list_location(f"{directory}/")

    def _row_carries_release(self) -> bool:
        row = self._port.read_profile_row(self._compiled.channel.registry, self._compiled.config_name)
        return row is not None and _carries(desktop_view(row), self._version, self._content)


def _refusal(
    key: str,
    state_entry: AppliedEntry | None,
    compiled: CompiledCocoDesktopProfile,
    observed: _Observed,
    observation: CompositeObservation,
) -> CompositePlan | None:
    """Block the plan when the stage, registry, or row is not one SST may write.

    Diagnostics:
        SST-PLN026: the profile stage encrypts client-side.
        SST-PLN029: the registry lacks a column SST writes, or has it with another type.
        SST-PLN024: the row exists, but state does not record it as SST's.
        SST-PLN028: the row carries a VERSION state did not record, and content other than this release's.
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
            D(
                "SST-PLN024",
                subject=key,
                artifact=key,
                value=(
                    f"{compiled.channel.registry.sql} row '{compiled.config_name}' (if another target or project "
                    "writes this CONFIG_NAME, give this one its own coco_desktop_profiles.+name_suffix)"
                ),
            ),
            ChangeReason.UNMANAGED_OBJECT,
        )
    recorded = dict(state_entry.component_fingerprints).get("version")
    if not same_version(observed.row_version, recorded) and observed.row_content != release_content(compiled):
        return blocked(
            observation,
            D(
                "SST-PLN028",
                subject=key,
                artifact=key,
                value=compiled.config_name,
                found=observed.row_version or "NULL",
                expected=recorded or "nothing",
            ),
        )
    return None


def _action(
    artifact: RenderedArtifact,
    state_entry: AppliedEntry | None,
    compiled: CompiledCocoDesktopProfile,
    observed: _Observed,
) -> tuple[Action, ChangeReason]:
    """Decide what an unblocked profile needs: its row, its trees, or only a state record.

    The row needs writing when its content differs, it is inactive, or its VERSION is not yet
    a counter; state needs writing when it records another release or another VERSION.
    """
    content = release_content(compiled)
    version = observed.version_for(content)
    # A `deactivated` entry is the tombstone SST's own --prune left: the row is
    # still SST's, carrying the VERSION it deactivated, and can be reactivated.
    retired = state_entry is not None and state_entry.outcome == DEACTIVATED
    complete = all(observed.trees.get(tree.prefix) == tuple(sorted(tree.paths)) for tree in compiled.release.trees)
    if not observed.row_exists:
        return Action.CREATE, ChangeReason.NOT_PRESENT
    if not same_version(observed.row_version, version) or not observed.row_active or not complete:
        return Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS
    recorded = dict(state_entry.component_fingerprints).get("version") if state_entry is not None else None
    if state_entry is not None and (
        state_entry.fingerprint != artifact.fingerprint
        or retired
        or state_entry.outcome == FAILED_AFTER_WRITE
        or not same_version(recorded, version)
    ):
        return Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH
    return Action.NOOP, ChangeReason.UNCHANGED


def row_content(view: Mapping[str, object]) -> str:
    """Digest what Desktop reads from a row, VERSION aside: two rows with one digest load alike."""
    columns = {column: value for column, value in view.items() if column != "VERSION"}
    return sha256(json.dumps(columns, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def release_content(compiled: CompiledCocoDesktopProfile) -> str:
    """Digest what Desktop will read from the release's row, as `row_content` reads a live one."""
    return row_content(desktop_view(compiled.release.row))


def _carries(view: Mapping[str, object], version: str, content: str) -> bool:
    return str(view.get("VERSION")) == version and row_content(view) == content


def _with_version(fingerprints: tuple[tuple[str, str], ...], version: str) -> tuple[tuple[str, str], ...]:
    return (*((key, value) for key, value in fingerprints if key != "version"), ("version", version))


def _refused(change: Change, detail: str, attempts: int) -> ApplyOutcome:
    """Fail a deactivation SST cannot vouch for (SST-APL012); retrying the statement cannot help."""
    error = ClassifiedError("SST-APL012", detail, match_signature(detail).kind)
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
