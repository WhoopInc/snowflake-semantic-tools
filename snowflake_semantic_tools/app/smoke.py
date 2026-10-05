"""Execute rendered smoke probes separately from publication.

`RunSmokeSuite` runs probes. `SmokePublished` is the smoke test suite's use case: it first
proves SST owns each object it would probe, from authoritative state and the object's live
ownership marker, and runs the probes only once it does. Given a `Fanout`, both the marker
reads and the probes run on sessions of their own; what they report is merged in probe
order, so it is the same for any thread count.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import Manifest, State


class SmokePort(CatalogPort, ExecutionPort, StatePort, Protocol):
    """The Snowflake roles the smoke suite over published objects uses: markers, probes, and state."""


@dataclass(frozen=True, slots=True)
class SmokeResult:
    """The probes a smoke run attempted, in order, with what they reported."""

    attempted: tuple[SmokeProbe, ...]
    diagnostics: DiagnosticBag

    @property
    def success(self) -> bool:
        """Report whether no diagnostic is an error."""
        return not self.diagnostics.has_errors


class RunSmokeSuite:
    """Run each rendered artifact's smoke probes, which read and never write.

    Args:
        readers: Sessions, opened from `port`, to run the probes on concurrently; None
            runs them on `port`, one at a time. `fail_fast` always runs them one at a time.
    """

    def __init__(self, port: ExecutionPort, readers: Fanout[ExecutionPort] | None = None) -> None:
        self._port = port
        self._readers = readers

    def run(self, rendered: tuple[RenderedArtifact, ...], *, fail_fast: bool = False) -> SmokeResult:
        """Run every probe, in artifact order; with `fail_fast`, stop at the first that fails.

        Diagnostics:
            SST-PLN100: a metric's probe failed.
            SST-APL100: any other probe failed.
            SST-APL006: after the failures, how many probes failed.
            SST-PLN101: with `fail_fast`, an artifact whose probes did not run once one failed.
        """
        if not fail_fast:
            return self._run_all(rendered)
        attempted: list[SmokeProbe] = []
        failures: list[Diagnostic] = []
        skipped: list[Diagnostic] = []
        for artifact in rendered:
            if failures and fail_fast:
                if artifact.smoke:
                    skipped.append(_skipped(artifact, "--fail-fast stopped the suite at an earlier failure"))
                continue
            for probe in artifact.smoke:
                attempted.append(probe)
                try:
                    self._port.query(probe.sql)
                except SnowflakePortError as exc:
                    failures.append(_probe_failure(artifact, probe, str(exc)))
                    if fail_fast:
                        break
        tally = (D("SST-APL006", count=len(failures)),) if failures else ()
        return SmokeResult(tuple(attempted), DiagnosticBag((*failures, *tally, *skipped)))

    def _run_all(self, rendered: tuple[RenderedArtifact, ...]) -> SmokeResult:
        """Run every probe, concurrently when given sessions, and report failures in probe order."""
        probes = tuple((artifact, probe) for artifact in rendered for probe in artifact.smoke)
        errors = (self._readers or Fanout(self._port)).map(_probe_error, probes)
        failures = tuple(
            _probe_failure(artifact, probe, error)
            for (artifact, probe), error in zip(probes, errors, strict=True)
            if error is not None
        )
        tally = (D("SST-APL006", count=len(failures)),) if failures else ()
        return SmokeResult(tuple(probe for _, probe in probes), DiagnosticBag((*failures, *tally)))


def _probe_error(port: ExecutionPort, item: tuple[RenderedArtifact, SmokeProbe]) -> str | None:
    """Run one probe; why it failed, or None when it answered."""
    try:
        port.query(item[1].sql)
    except SnowflakePortError as exc:
        return str(exc)
    return None


def _probe_failure(artifact: RenderedArtifact, probe: SmokeProbe, detail: str) -> Diagnostic:
    """Report a failed probe: a metric's as SST-PLN100, naming the metric; any other as SST-APL100."""
    if probe.kind is ProbeKind.METRIC:
        member = probe.key.split(":", 1)[-1]
        return D("SST-PLN100", artifact=artifact.key, member=member, detail=detail, subject=probe.key)
    return D("SST-APL100", artifact=probe.key, detail=detail, subject=artifact.key)


def _skipped(artifact: RenderedArtifact, why: str) -> Diagnostic:
    return D("SST-PLN101", artifact=artifact.key, detail=why, subject=artifact.key)


def unprobed_metrics(result: CompileResult) -> tuple[Diagnostic, ...]:
    """Report each public metric that has no smoke probe, because no query can name it.

    Diagnostics:
        SST-PLN020: a public metric of a semantic view has no probe.
    """
    found: list[Diagnostic] = []
    for item in result.compiled:
        if not isinstance(item, CompiledView):
            continue
        probed = {probe.key for probe in item.rendered_artifact.smoke}
        for metric in item.view.metrics:
            key = artifact_key("metric", metric.qualified_name.casefold())
            if metric.access_modifier != "private_access" and key not in probed:
                found.append(D("SST-PLN020", artifact=item.artifact_key, member=metric.qualified_name, subject=key))
    return tuple(found)


class SmokePublished:
    """Probe the published objects of a compiled project, once SST is proven to own each one.

    Ownership is proven from authoritative state and each object's live ownership marker, so
    no probe runs against an object some other run, or someone else, published.

    Args:
        readers: Sessions, opened from `port`, to read the markers and run the probes on
            concurrently; None runs them on `port`, one at a time.
    """

    def __init__(self, port: SmokePort, state_store: StateStore, readers: Fanout[SmokePort] | None = None) -> None:
        self._port = port
        self._state_store = state_store
        self._readers = readers

    def run(
        self,
        result: CompileResult,
        manifest: Manifest,
        *,
        target: TargetIdentity,
        state_table: QualifiedName,
        fail_fast: bool = False,
    ) -> SmokeResult:
        """Prove SST owns each object with probes, then probe every artifact as apply published it.

        Steps, in order:

        1. Read authoritative state, as `read_state` does.
        2. Check the state was applied from `manifest`.
        3. Render each artifact as apply published it for `manifest`.
        4. Check each artifact with probes: state records it from `manifest`, with its
           fingerprint and target, and its live marker is the one apply wrote. Composite
           artifacts carry no marker and have no probe, so they are not asked for one.
        5. Check every public metric has a probe.
        6. Run the probes only when nothing was reported; otherwise probe nothing.

        Args:
            manifest: The manifest the project compiles to, which `sst compile` wrote.

        Diagnostics:
            SST-APL012: state was not applied from `manifest`, or an object with probes is
                not the one apply published.
            Those of `read_state`, any of which also keeps the probes from running, and the
            probes' own.
        """
        state, state_diagnostics = read_state(self._state_store, self._port, state_table=state_table, target=target)
        problems: list[Diagnostic] = list(state_diagnostics)
        if state.manifest_id != manifest.manifest_id:
            problems.append(
                D("SST-APL012", artifact="manifest", value="authoritative state does not match the compiled manifest")
            )
        published = {artifact.key: artifact for artifact in result.rendered_for_publish(manifest.manifest_id)}
        problems.extend(self._unowned(result, manifest, state))
        problems.extend(unprobed_metrics(result))
        if problems:
            return SmokeResult((), DiagnosticBag(tuple(problems)))
        return RunSmokeSuite(self._port, self._readers).run(tuple(published.values()), fail_fast=fail_fast)

    def _unowned(self, result: CompileResult, manifest: Manifest, state: State) -> list[Diagnostic]:
        """Report each artifact with probes whose state entry or live marker is not apply's, in compile order.

        The live marker is read only for an artifact whose state entry matches.
        """
        probed = tuple(item for item in result.rendered if item.smoke)
        recorded = tuple(artifact for artifact in probed if _recorded(artifact, manifest, state))
        markers = dict(
            zip(
                (artifact.key for artifact in recorded),
                (self._readers or Fanout(self._port)).map(
                    lambda port, artifact: port.describe_marker(artifact.target, artifact.object_type), recorded
                ),
                strict=True,
            )
        )
        return [
            D("SST-APL012", artifact=artifact.key, value=artifact.target.sql)
            for artifact in probed
            if markers.get(artifact.key) != OwnershipMarker(manifest.manifest_id, artifact.fingerprint)
        ]


def _recorded(artifact: RenderedArtifact, manifest: Manifest, state: State) -> bool:
    """Report whether state records the artifact as applied from `manifest`, at its fingerprint and target."""
    entry = state.applied.get(artifact.key)
    return (
        entry is not None
        and entry.fingerprint == artifact.fingerprint
        and entry.manifest_id == manifest.manifest_id
        and QualifiedName.parse(entry.qualified_name).folded == artifact.target.folded
    )
