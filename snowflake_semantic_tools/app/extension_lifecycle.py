"""Plan and publish content-addressed Cortex Extension versions for skills and plugins.

The version alias is the bundle digest, so a version is never rewritten: apply
uploads the bundle under `@<stage>/<name>/<ALIAS>/`, verifies it byte for byte,
and adds the version from that prefix only when the alias is absent. It never
drops, grants, un-certifies, or changes discoverability.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
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
from ..domain.ports.snowflake import ExtensionObservation, ExtensionVersion, SnowflakePort, SnowflakePortError
from ..domain.state.model import FAILED_AFTER_WRITE, AppliedEntry, AppliedResourceInput, Manifest
from .apply import classify_error
from .skill_compile import ExtensionRelease

SSE_STAGE_TYPE = "INTERNAL NO CSE"
CERTIFIED = "CERTIFIED"


@dataclass(frozen=True, slots=True)
class _Observed:
    stage_type: str | None
    staged: tuple[str, ...]
    extension: ExtensionObservation | None
    version: ExtensionVersion | None
    version_files: tuple[str, ...]

    def details(self) -> tuple[tuple[str, str], ...]:
        extension = self.extension
        version = self.version
        return (
            ("stage_type", self.stage_type or ""),
            ("staged", _digest(self.staged)),
            ("extension_type", extension.extension_type if extension else ""),
            ("comment", _text_digest(extension.comment) if extension and extension.comment is not None else ""),
            ("version", version.name if version else ""),
            ("version_files", _digest(self.version_files)),
            ("certification", (version.certification_status or "") if version else ""),
        )


class ExtensionLifecycleHandler:
    """One handler per extension-backed artifact type (`skill` or `plugin`)."""

    def __init__(self, port: SnowflakePort, releases: Mapping[str, ExtensionRelease], artifact_type: str) -> None:
        self._port = port
        self._releases = MappingProxyType(dict(releases))
        self.artifact_type = artifact_type
        self._stage_lock = Lock()

    def plan(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        manifest: Manifest,
    ) -> CompositePlan:
        del manifest
        release = self._releases[artifact.key]
        try:
            observed = self._observe(release)
        except SnowflakePortError as exc:
            diagnostic = D("SST-PLN001", value=artifact.key, detail=str(exc))
            empty = CompositeObservation(artifact.key, diagnostics=DiagnosticBag((diagnostic,)))
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, empty, empty.diagnostics)
        observation = self._observation(release, observed)
        if observed.stage_type is not None and observed.stage_type.upper() != SSE_STAGE_TYPE:
            return self._blocked(
                observation,
                ChangeReason.VALIDATION_ERRORS,
                D(
                    "SST-PLN026",
                    subject=artifact.key,
                    artifact=artifact.key,
                    value=release.stage.sql,
                    found=observed.stage_type,
                ),
            )
        extension = observed.extension
        if extension is not None:
            if state_entry is None or not _recorded_target(state_entry, release):
                return self._blocked(
                    observation,
                    ChangeReason.UNMANAGED_OBJECT,
                    D("SST-PLN024", subject=artifact.key, artifact=artifact.key, value=release.target.sql),
                )
            if extension.extension_type != release.extension_type:
                return self._blocked(
                    observation,
                    ChangeReason.UNMANAGED_OBJECT,
                    D(
                        "SST-PLN002",
                        subject=artifact.key,
                        artifact=artifact.key,
                        value=release.target.sql,
                        found=f"{extension.extension_type} extension",
                    ),
                )
        diagnostics: list[Diagnostic] = []
        version = observed.version
        if version is not None:
            if observed.version_files != tuple(sorted(release.paths)):
                return self._blocked(
                    observation,
                    ChangeReason.VALIDATION_ERRORS,
                    D(
                        "SST-PLN027",
                        subject=artifact.key,
                        artifact=artifact.key,
                        value=release.alias,
                        detail=_difference(observed.version_files, release.paths),
                    ),
                )
            if not version.is_default:
                diagnostics.append(
                    D(
                        "SST-VAL841",
                        subject=artifact.key,
                        artifact=artifact.key,
                        value=release.alias,
                        target=release.target.sql,
                    )
                )
        comment_drift = extension is not None and (extension.comment or "") != release.comment
        uncertified = release.certified and (version is None or version.certification_status != CERTIFIED)
        if extension is None:
            action, reason = Action.CREATE, ChangeReason.NOT_PRESENT
        elif version is None or comment_drift or uncertified:
            action, reason = Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS
        elif state_entry is not None and (
            state_entry.fingerprint != artifact.fingerprint or state_entry.outcome == FAILED_AFTER_WRITE
        ):
            # A revert to a version that already exists, or a publish that failed
            # after writing it: nothing is published, but the run re-verifies the
            # version and state records which one the project now declares.
            action, reason = Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH
        else:
            action, reason = Action.NOOP, ChangeReason.UNCHANGED
        return CompositePlan(action, reason, observation, DiagnosticBag(diagnostics))

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        del options
        artifact = change.rendered
        if change.action in (Action.PRUNE, Action.NOOP, Action.BLOCKED) or artifact is None:
            return ApplyOutcome(
                change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, artifact.ddl if artifact else ""
            )
        release = self._releases[change.key]
        current = self._observe(release)
        planned = change.composite_observation
        if planned is None or _stale(planned.details, self._observation(release, current).details):
            return self._failure(change, f"{release.target.sql} changed since the plan", code="SST-APL012")
        run = _Run(self, change, release)
        return run.publish(current)

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        """Prune is report-only: SST never drops an extension or a version."""
        resources = ", ".join(resource.qualified_name for resource in state_entry.applied_resources) or artifact_key
        diagnostic = D("SST-PLN021", count=len(state_entry.applied_resources), value=resources)
        return Change(
            artifact_key,
            self.artifact_type,
            Action.PRUNE,
            ChangeReason.ORPHANED,
            None,
            None,
            (),
            250 if self.artifact_type == "skill" else 260,
            DiagnosticBag((diagnostic,)),
            prune_executable=False,
        )

    @staticmethod
    def merge_physical_resources(
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[AppliedResourceInput, ...]:
        del previous
        return current

    def _observe(self, release: ExtensionRelease) -> _Observed:
        stage_type = self._port.stage_type(release.stage)
        staged = self._port.list_location(release.prefix) if stage_type is not None else ()
        extension = self._port.observe_extension(release.target)
        version = None
        version_files: tuple[str, ...] = ()
        if extension is not None:
            version = next(
                (
                    item
                    for item in self._port.extension_versions(release.target)
                    if (item.alias or "").casefold() == release.alias.casefold()
                ),
                None,
            )
            if version is not None:
                version_files = self._port.list_location(version.location)
        return _Observed(stage_type, tuple(sorted(staged)), extension, version, tuple(sorted(version_files)))

    def _observation(self, release: ExtensionRelease, observed: _Observed) -> CompositeObservation:
        return CompositeObservation(
            key=release.key,
            resources=(
                PhysicalResource("CORTEX EXTENSION", release.target, observed.extension is not None),
                PhysicalResource("STAGE", release.stage, observed.stage_type is not None),
            ),
            stage_exists=observed.stage_type is not None,
            config_path=release.prefix,
            config_exists=bool(observed.staged),
            details=observed.details(),
        )

    @staticmethod
    def _blocked(observation: CompositeObservation, reason: ChangeReason, diagnostic: Diagnostic) -> CompositePlan:
        return CompositePlan(Action.BLOCKED, reason, observation, DiagnosticBag((diagnostic,)))

    def _failure(
        self,
        change: Change,
        detail: str,
        *,
        code: str = "SST-APL001",
        write_succeeded: bool = False,
        attempts: int = 0,
        physical_resources: tuple[tuple[str, str], ...] = (),
    ) -> ApplyOutcome:
        artifact = change.rendered
        classified = classify_error(detail)
        return ApplyOutcome(
            change.key,
            change.action,
            OutcomeStatus.FAILED,
            attempts,
            0,
            artifact.ddl if artifact is not None else "",
            ClassifiedError(code, detail, classified.kind, classified.retryable, classified.sqlstate),
            write_succeeded=write_succeeded,
            component_fingerprints=artifact.component_fingerprints if artifact is not None else (),
            physical_resources=physical_resources,
        )


class _Run:
    """One publication attempt, tracking what has been written so a failure reports it."""

    def __init__(self, handler: ExtensionLifecycleHandler, change: Change, release: ExtensionRelease) -> None:
        self._handler = handler
        self._port = handler._port
        self._change = change
        self._release = release
        self._attempts = 0
        self._written = False
        self._resources: list[tuple[str, str]] = []

    def publish(self, current: _Observed) -> ApplyOutcome:
        # A read-back error after CREATE must still report the write, or state would
        # forget the extension and every later plan would call it unmanaged.
        try:
            return self._publish(current)
        except SnowflakePortError as exc:
            return self._fail(f"{exc}")

    def _publish(self, current: _Observed) -> ApplyOutcome:
        release = self._release
        stage_failure = self._ensure_stage(current)
        if stage_failure is not None:
            return stage_failure
        staged = set(current.staged)
        for entry in release.bundle.entries:
            if entry.path in staged:
                continue
            self._attempts += 1
            try:
                self._port.upload(f"{release.prefix}{entry.path}", entry.content)
            except SnowflakePortError as exc:
                return self._fail(f"upload of {entry.path} failed: {exc}")
            self._written = True
        listed = tuple(sorted(self._port.list_location(release.prefix)))
        if listed != tuple(sorted(release.paths)):
            return self._fail(
                f"{release.prefix} differs from the bundle ({_difference(listed, release.paths)})", "SST-APL016"
            )
        for entry in release.bundle.entries:
            if self._port.read_staged_file(f"{release.prefix}{entry.path}") != entry.content:
                return self._fail(f"{release.prefix}{entry.path} does not read back byte for byte", "SST-APL016")
        target = release.target.sql
        if current.extension is None:
            failure = self._execute(
                f"CREATE CORTEX EXTENSION IF NOT EXISTS {target} TYPE = '{release.extension_type}' "
                f"COMMENT = {_sql_string(release.comment)}"
            )
            if failure is not None:
                return failure
        self._resources.append(("CORTEX EXTENSION", target))
        if current.version is None:
            failure = self._add_version()
            if failure is not None:
                return failure
        version = self._version()
        if version is None:
            return self._fail(f"alias {release.alias} is absent from {target} after ADD VERSION", "SST-APL016")
        published = tuple(sorted(self._port.list_location(version.location)))
        if published != tuple(sorted(release.paths)):
            return self._fail(
                f"{version.name} of {target} differs from the bundle ({_difference(published, release.paths)})",
                "SST-APL016",
            )
        if current.extension is not None and (current.extension.comment or "") != release.comment:
            failure = self._execute(f"ALTER CORTEX EXTENSION {target} SET COMMENT = {_sql_string(release.comment)}")
            if failure is not None:
                return failure
        if release.certified and version.certification_status != CERTIFIED:
            failure = self._execute(
                f"ALTER CORTEX EXTENSION {target} VERSION {release.alias} "
                "SET TAG SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'",
                code="SST-APL007",
            )
            if failure is not None:
                return failure
            version = self._version()
            if version is None or version.certification_status != CERTIFIED:
                found = version.certification_status if version is not None else "absent"
                return self._fail(
                    f"{release.alias} of {target} reports certification {found or 'unset'} after tagging",
                    "SST-APL007",
                )
        artifact = self._change.rendered
        assert artifact is not None
        return ApplyOutcome(
            self._change.key,
            self._change.action,
            OutcomeStatus.APPLIED,
            max(self._attempts, 1),
            0,
            artifact.ddl,
            write_succeeded=self._written or self._change.action is Action.UPDATE,
            component_fingerprints=(*artifact.component_fingerprints, ("version", version.name)),
            physical_resources=(("CORTEX EXTENSION", target), ("STAGE", release.stage.sql)),
        )

    def _ensure_stage(self, current: _Observed) -> ApplyOutcome | None:
        stage = self._release.stage
        if current.stage_type is not None:
            self._resources.append(("STAGE", stage.sql))
            return None
        # Several artifacts share one bundle stage; create it once.
        with self._handler._stage_lock:
            if self._port.stage_type(stage) is None:
                failure = self._execute(f"CREATE STAGE IF NOT EXISTS {stage.sql} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')")
                if failure is not None:
                    return failure
            found = self._port.stage_type(stage)
        if found is None or found.upper() != SSE_STAGE_TYPE:
            return self._fail(f"stage {stage.sql} is {found or 'absent'} after creation", "SST-APL016")
        self._resources.append(("STAGE", stage.sql))
        return None

    def _add_version(self) -> ApplyOutcome | None:
        release = self._release
        target = release.target.sql
        self._attempts += 1
        result = self._port.execute_script(
            (f"ALTER CORTEX EXTENSION {target} ADD VERSION {release.alias} FROM {release.prefix}",)
        )
        if result.ok:
            self._written = True
            return None
        detail = result.error.message if result.error else "ADD VERSION failed"
        if release.extension_type != "PLUGIN":
            return self._fail(f"ADD VERSION failed: {detail}")
        return self._add_live_version()

    def _add_live_version(self) -> ApplyOutcome | None:
        """The PLUGIN fallback: a LIVE version built from empty, never `FROM LAST`."""
        release = self._release
        target = release.target.sql
        self._port.execute_script((f"ALTER CORTEX EXTENSION {target} ABORT",))
        failure = self._execute(f"ALTER CORTEX EXTENSION {target} ADD LIVE VERSION {release.alias}")
        if failure is not None:
            return failure
        live = f"snow://cortex_extension/{target}/versions/live/"
        for entry in release.bundle.entries:
            self._attempts += 1
            try:
                self._port.upload(f"{live}{entry.path}", entry.content)
            except SnowflakePortError as exc:
                self._port.execute_script((f"ALTER CORTEX EXTENSION {target} ABORT",))
                return self._fail(f"upload of {entry.path} into the live version failed: {exc}")
        failure = self._execute(f"ALTER CORTEX EXTENSION {target} COMMIT")
        if failure is not None:
            self._port.execute_script((f"ALTER CORTEX EXTENSION {target} ABORT",))
        return failure

    def _version(self) -> ExtensionVersion | None:
        alias = self._release.alias.casefold()
        versions = self._port.extension_versions(self._release.target)
        return next((item for item in versions if (item.alias or "").casefold() == alias), None)

    def _execute(self, statement: str, *, code: str = "SST-APL001") -> ApplyOutcome | None:
        self._attempts += 1
        result = self._port.execute_script((statement,))
        if not result.ok:
            detail = result.error.message if result.error else "statement failed"
            return self._fail(f"{statement.split(' FROM ')[0][:120]} failed: {detail}", code)
        self._written = True
        return None

    def _fail(self, detail: str, code: str = "SST-APL001") -> ApplyOutcome:
        # State takes ownership only of an extension that exists. Uploads alone are
        # content-addressed leftovers, and recording them would let a retry treat a
        # same-named extension someone else created as SST's.
        owned = ("CORTEX EXTENSION", self._release.target.sql) in self._resources
        return self._handler._failure(
            self._change,
            detail,
            code=code,
            write_succeeded=self._written and owned,
            attempts=self._attempts,
            physical_resources=tuple(dict.fromkeys(self._resources)),
        )


def _recorded_target(entry: AppliedEntry, release: ExtensionRelease) -> bool:
    try:
        return QualifiedName.parse(entry.qualified_name).folded == release.target.folded
    except ValueError:
        return False


def _stale(planned: tuple[tuple[str, str], ...], current: tuple[tuple[str, str], ...]) -> bool:
    """Changed since plan, ignoring a sibling artifact creating the shared bundle stage."""
    before = dict(planned)
    after = dict(current)
    if before.get("stage_type") == "" and after.get("stage_type") == SSE_STAGE_TYPE:
        before["stage_type"] = SSE_STAGE_TYPE
    return before != after


def _difference(found: tuple[str, ...], expected: tuple[str, ...]) -> str:
    missing = sorted(set(expected) - set(found))
    unexpected = sorted(set(found) - set(expected))
    parts = []
    if missing:
        parts.append(f"missing {', '.join(missing[:3])}{' ...' if len(missing) > 3 else ''}")
    if unexpected:
        parts.append(f"unexpected {', '.join(unexpected[:3])}{' ...' if len(unexpected) > 3 else ''}")
    return "; ".join(parts) or "file sets differ"


def _digest(paths: tuple[str, ...]) -> str:
    return sha256("\n".join(sorted(paths)).encode("utf-8")).hexdigest()


def _text_digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _sql_string(value: str) -> str:
    """A single-quoted Snowflake literal: backslash is an escape inside one."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
