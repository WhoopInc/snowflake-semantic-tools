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

from ...domain.model.diagnostic import D, DiagnosticBag
from ...domain.model.identifier import QualifiedName
from ...domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ChangeReason,
    CompositeObservation,
    CompositePlan,
    PhysicalResource,
    RenderedArtifact,
)
from ...domain.model.sql import string_literal
from ...domain.ports.snowflake import ExtensionObservation, ExtensionVersion, SnowflakePort, SnowflakePortError
from ...domain.state import FAILED_AFTER_WRITE, AppliedEntry
from ..compile.skills import ExtensionRelease
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

CERTIFIED = "CERTIFIED"


@dataclass(frozen=True, slots=True)
class _Observed:
    stage_type: str | None
    staged: tuple[str, ...]
    extension: ExtensionObservation | None
    version: ExtensionVersion | None
    version_files: tuple[str, ...]
    versions: tuple[ExtensionVersion, ...] = ()

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


class ExtensionLifecycleHandler(CompositeHandler[ExtensionRelease, _Observed]):
    """One handler per extension-backed artifact type (`skill` or `plugin`).

    Prune is report-only: SST never drops an extension or a version.
    """

    _prune_detail = "remove the extension by hand once nothing uses it"

    def __init__(self, port: SnowflakePort, releases: Mapping[str, ExtensionRelease], artifact_type: str) -> None:
        super().__init__(port)
        self._releases = MappingProxyType(dict(releases))
        self.artifact_type = artifact_type
        self._prune_order = 250 if artifact_type == "skill" else 260
        self._stage_lock = Lock()

    def _subject(self, artifact: RenderedArtifact) -> ExtensionRelease:
        return self._releases[artifact.key]

    def _observe(self, release: ExtensionRelease) -> _Observed:
        stage_type = self._port.stage_type(release.stage)
        staged = self._port.list_location(release.prefix) if stage_type is not None else ()
        extension = self._port.observe_extension(release.target)
        version = None
        version_files: tuple[str, ...] = ()
        versions: tuple[ExtensionVersion, ...] = ()
        if extension is not None:
            versions = self._port.extension_versions(release.target)
            version = next(
                (item for item in versions if (item.alias or "").casefold() == release.alias.casefold()),
                None,
            )
            if version is not None:
                version_files = self._port.list_location(version.location)
        return _Observed(stage_type, tuple(sorted(staged)), extension, version, tuple(sorted(version_files)), versions)

    def _decide(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        release: ExtensionRelease,
        observed: _Observed,
    ) -> CompositePlan:
        observation = self._observation(release, observed)
        refusal = _refusal(artifact.key, state_entry, release, observed, observation)
        if refusal is not None:
            return refusal
        warnings = _served_warning(artifact.key, observed, release)
        action, reason = _action(artifact, state_entry, release, observed)
        return CompositePlan(action, reason, observation, warnings)

    def _publish(self, change: Change, artifact: RenderedArtifact) -> ApplyOutcome:
        release = self._releases[change.key]
        current = self._observe(release)
        planned = change.composite_observation
        if planned is None or _stale(planned.details, self._observation(release, current).details):
            return failed(change, f"{release.target.sql} changed since the plan", code="SST-APL012")
        return _Run(self._port, change, artifact, release, current, self._stage_lock).publish()

    def _apply_prune(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        """Skip a prune, which is report-only, with the artifact's text if the change carries one."""
        del options
        return skipped(change, change.rendered.ddl if change.rendered is not None else "")

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


class _Run(PublicationRun):
    """One publication attempt, tracking what has been written so a failure reports it.

    The steps run in order: ensure the bundle stage, upload the bundle and read it back,
    create the extension, add the version, then verify the version, align the comment,
    and certify it.
    """

    def __init__(
        self,
        port: SnowflakePort,
        change: Change,
        artifact: RenderedArtifact,
        release: ExtensionRelease,
        current: _Observed,
        stage_lock: Lock,
    ) -> None:
        super().__init__(port, change, artifact)
        self._release = release
        self._current = current
        self._stage_lock = stage_lock

    def publish(self) -> ApplyOutcome:
        """Publish the release, reporting a port error as a failure that keeps what was written."""
        # A read-back error after CREATE must still report the write, or state would
        # forget the extension and every later plan would call it unmanaged.
        try:
            failure = self._run_steps(
                self._ensure_stage,
                self._upload_bundle,
                self._verify_bundle,
                self._create_extension,
                self._add_version,
            )
            return failure if failure is not None else self._check_version()
        except SnowflakePortError as exc:
            return self.fail(f"{exc}")

    def _owns_write(self) -> bool:
        # State takes ownership only of an extension that exists. Uploads alone are
        # content-addressed leftovers, and recording them would let a retry treat a
        # same-named extension someone else created as SST's.
        return self._written and ("CORTEX EXTENSION", self._release.target.sql) in self._verified

    def _ensure_stage(self) -> ApplyOutcome | None:
        stage = self._release.stage
        if self._current.stage_type is not None:
            self._verified.append(("STAGE", stage.sql))
            return None
        # Several artifacts share one bundle stage; create it once.
        with self._stage_lock:
            if self._port.stage_type(stage) is None:
                failure = self._execute(create_sse_stage_sql(stage))
                if failure is not None:
                    return failure
            found = self._port.stage_type(stage)
        if found is None or found.upper() != SSE_STAGE_TYPE:
            return self.fail(f"stage {stage.sql} is {found or 'absent'} after creation", "SST-APL016")
        self._verified.append(("STAGE", stage.sql))
        return None

    def _upload_bundle(self) -> ApplyOutcome | None:
        release = self._release
        failed_upload = self._upload_missing(release.prefix, release.bundle.entries, set(self._current.staged))
        if failed_upload is None:
            return None
        entry, error = failed_upload
        return self.fail(f"upload of {entry.path} failed: {error}")

    def _verify_bundle(self) -> ApplyOutcome | None:
        release = self._release
        return self._verify_staged(
            release.prefix,
            release.bundle.entries,
            lambda listed: f"{release.prefix} differs from the bundle ({_difference(listed, release.paths)})",
        )

    def _create_extension(self) -> ApplyOutcome | None:
        release = self._release
        target = release.target.sql
        if self._current.extension is None:
            failure = self._execute(
                f"CREATE CORTEX EXTENSION IF NOT EXISTS {target} TYPE = '{release.extension_type}' "
                f"COMMENT = {string_literal(release.comment)}"
            )
            if failure is not None:
                return failure
        self._verified.append(("CORTEX EXTENSION", target))
        return None

    def _add_version(self) -> ApplyOutcome | None:
        if self._current.version is not None:
            return None
        release = self._release
        result = self._run_statement(
            f"ALTER CORTEX EXTENSION {release.target.sql} ADD VERSION {release.alias} FROM {release.prefix}"
        )
        if result.ok:
            return None
        detail = result.error.message if result.error else "ADD VERSION failed"
        if release.extension_type != "PLUGIN":
            return self.fail(f"ADD VERSION failed: {detail}")
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
        failed_upload = self._upload_missing(live, release.bundle.entries, ())
        if failed_upload is not None:
            entry, error = failed_upload
            self._port.execute_script((f"ALTER CORTEX EXTENSION {target} ABORT",))
            return self.fail(f"upload of {entry.path} into the live version failed: {error}")
        failure = self._execute(f"ALTER CORTEX EXTENSION {target} COMMIT")
        if failure is not None:
            self._port.execute_script((f"ALTER CORTEX EXTENSION {target} ABORT",))
        return failure

    def _check_version(self) -> ApplyOutcome:
        """Require the aliased version to hold the bundle, align the comment, then certify if asked."""
        release = self._release
        target = release.target.sql
        version = self._version()
        if version is None:
            return self.fail(f"alias {release.alias} is absent from {target} after ADD VERSION", "SST-APL016")
        published = tuple(sorted(self._port.list_location(version.location)))
        if published != tuple(sorted(release.paths)):
            return self.fail(
                f"{version.name} of {target} differs from the bundle ({_difference(published, release.paths)})",
                "SST-APL016",
            )
        failure = self._align_comment()
        if failure is not None:
            return failure
        if release.certified and not _certified(self._current.extension, version):
            return self._certify()
        return self._published(version)

    def _align_comment(self) -> ApplyOutcome | None:
        extension = self._current.extension
        release = self._release
        if extension is None or (extension.comment or "") == release.comment:
            return None
        return self._execute(
            f"ALTER CORTEX EXTENSION {release.target.sql} SET COMMENT = {string_literal(release.comment)}"
        )

    def _certify(self) -> ApplyOutcome:
        """Tag the version CERTIFIED, then require the catalog to report it certified (SST-APL007)."""
        release = self._release
        target = release.target.sql
        failure = self._execute(
            f"ALTER CORTEX EXTENSION {target} VERSION {release.alias} "
            "SET TAG SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'",
            code="SST-APL007",
        )
        if failure is not None:
            return failure
        version = self._version()
        if version is None or not _certified(self._port.observe_extension(release.target), version):
            found = version.certification_status if version is not None else "absent"
            return self.fail(
                f"{release.alias} of {target} reports certification {found or 'unset'} after tagging",
                "SST-APL007",
            )
        return self._published(version)

    def _published(self, version: ExtensionVersion) -> ApplyOutcome:
        release = self._release
        return self.applied(
            write_succeeded=self._written or self._change.action is Action.UPDATE,
            component_fingerprints=(*self._artifact.component_fingerprints, ("version", version.name)),
            physical_resources=(("CORTEX EXTENSION", release.target.sql), ("STAGE", release.stage.sql)),
        )

    def _version(self) -> ExtensionVersion | None:
        alias = self._release.alias.casefold()
        versions = self._port.extension_versions(self._release.target)
        return next((item for item in versions if (item.alias or "").casefold() == alias), None)

    def _execute(self, statement: str, *, code: str = "SST-APL001") -> ApplyOutcome | None:
        result = self._run_statement(statement)
        if result.ok:
            return None
        detail = result.error.message if result.error else "statement failed"
        return self.fail(f"{statement.split(' FROM ')[0][:120]} failed: {detail}", code)


def _refusal(
    key: str,
    state_entry: AppliedEntry | None,
    release: ExtensionRelease,
    observed: _Observed,
    observation: CompositeObservation,
) -> CompositePlan | None:
    """Block the plan when the stage, extension, or version is not one SST may publish into.

    Diagnostics:
        SST-PLN026: the bundle stage encrypts client-side.
        SST-PLN024: the extension exists, but state does not record it as SST's.
        SST-PLN002: the extension exists with another type.
        SST-PLN027: the alias names a version whose files are not the bundle's.
    """
    if observed.stage_type is not None and observed.stage_type.upper() != SSE_STAGE_TYPE:
        return blocked(
            observation,
            D("SST-PLN026", subject=key, artifact=key, value=release.stage.sql, found=observed.stage_type),
        )
    extension = observed.extension
    if extension is not None:
        if state_entry is None or not _recorded_target(state_entry, release):
            return blocked(
                observation,
                D("SST-PLN024", subject=key, artifact=key, value=release.target.sql),
                ChangeReason.UNMANAGED_OBJECT,
            )
        if extension.extension_type != release.extension_type:
            return blocked(
                observation,
                D(
                    "SST-PLN002",
                    subject=key,
                    artifact=key,
                    value=release.target.sql,
                    found=f"{extension.extension_type} extension",
                ),
                ChangeReason.UNMANAGED_OBJECT,
            )
    if observed.version is not None and observed.version_files != tuple(sorted(release.paths)):
        return blocked(
            observation,
            D(
                "SST-PLN027",
                subject=key,
                artifact=key,
                value=release.alias,
                detail=_difference(observed.version_files, release.paths),
            ),
        )
    return None


def _action(
    artifact: RenderedArtifact,
    state_entry: AppliedEntry | None,
    release: ExtensionRelease,
    observed: _Observed,
) -> tuple[Action, ChangeReason]:
    """Decide what an unblocked release needs: the extension, its version, comment, or certification."""
    extension = observed.extension
    version = observed.version
    comment_drift = extension is not None and (extension.comment or "") != release.comment
    uncertified = release.certified and (version is None or not _certified(extension, version))
    if extension is None:
        return Action.CREATE, ChangeReason.NOT_PRESENT
    if version is None or comment_drift or uncertified:
        return Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS
    if state_entry is not None and (
        state_entry.fingerprint != artifact.fingerprint or state_entry.outcome == FAILED_AFTER_WRITE
    ):
        # A revert to a version that already exists, or a publish that failed
        # after writing it: nothing is published, but the run re-verifies the
        # version and state records which one the project now declares.
        return Action.UPDATE, ChangeReason.STATE_MANIFEST_MISMATCH
    return Action.NOOP, ChangeReason.UNCHANGED


def _served_warning(key: str, observed: _Observed, release: ExtensionRelease) -> DiagnosticBag:
    """Warn when the catalog will serve another version than this release's (SST-VAL841)."""
    served = _served_instead(observed, release)
    if served is None:
        return DiagnosticBag()
    return DiagnosticBag(
        (
            D(
                "SST-VAL841",
                subject=key,
                artifact=key,
                value=release.alias,
                target=release.target.sql,
                found=served[0],
                detail=served[1],
            ),
        )
    )


def _recorded_target(entry: AppliedEntry, release: ExtensionRelease) -> bool:
    try:
        return QualifiedName.parse(entry.qualified_name).folded == release.target.folded
    except ValueError:
        return False


def _certified(extension: ExtensionObservation | None, version: ExtensionVersion) -> bool:
    """Whether a version is certified, by its own status or as the extension's latest certified version.

    Either signal counts: some versions tagged by the extensions pipeline report an empty
    per-version status while the extension names them its latest certified one.
    """
    if (version.certification_status or "").upper() == CERTIFIED:
        return True
    latest = extension.latest_certified_version if extension is not None else None
    return latest is not None and latest.upper() == version.name.upper()


def _served_instead(observed: _Observed, release: ExtensionRelease) -> tuple[str, str] | None:
    """The version the catalog will serve instead of this release's, and why.

    The catalog serves the latest certified version when any is certified, and the
    default otherwise; ADD VERSION makes the new version the default. Agents pin
    their version, so only catalog users are affected.
    """
    extension = observed.extension
    if extension is None:
        return None
    certified = {item.name.upper() for item in observed.versions if _certified(extension, item)}
    ours = observed.version.name.upper() if observed.version is not None else None
    if release.certified:
        if ours is None:
            return None
        certified.add(ours)
    if certified:
        latest = max(certified, key=_version_number)
        if latest == ours:
            return None
        return latest, "it is the latest certified version"
    default = next((item.name.upper() for item in observed.versions if item.is_default), None)
    if ours is None or default is None or default == ours:
        return None
    return default, "it is the default version"


def _version_number(name: str) -> int:
    number = name.upper().removeprefix("VERSION$")
    return int(number) if number.isdigit() else -1


def _stale(planned: tuple[tuple[str, str], ...], current: tuple[tuple[str, str], ...]) -> bool:
    """Changed since plan, ignoring a sibling artifact creating the shared bundle stage."""
    return details_stale(planned, current, creatable={"stage_type": ("", SSE_STAGE_TYPE)})


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
