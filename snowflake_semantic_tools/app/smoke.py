"""Execute rendered smoke probes separately from publication.

`RunSmokeSuite` runs probes. `SmokePublished` is `sst test`'s use case: it first
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
from snowflake_semantic_tools.app.publication import Checked, Publication, unpublished_count, unpublished_findings
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.ports.progress import NO_PROGRESS, Progress
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.ports.timing import NO_PHASES, PhaseTimer
from snowflake_semantic_tools.domain.state import AppliedEntry, Manifest, State


class SmokePort(CatalogPort, ExecutionPort, StatePort, Protocol):
    """The Snowflake roles `sst test` uses over published objects: markers, probes, and state."""


@dataclass(frozen=True, slots=True)
class SmokeResult:
    """The probes a smoke run attempted, in order, with what they reported.

    Attributes:
        unpublished: How many artifacts with probes were not published as compiled, which kept
            every probe from running; 0 when they all were, or state could not be read.
    """

    attempted: tuple[SmokeProbe, ...]
    diagnostics: DiagnosticBag
    unpublished: int = 0

    @property
    def success(self) -> bool:
        """Report whether no diagnostic is an error."""
        return not self.diagnostics.has_errors


class RunSmokeSuite:
    """Run each rendered artifact's smoke probes, which read and never write.

    Args:
        readers: Sessions, opened from `port`, to run the probes on concurrently; None
            runs them on `port`, one at a time. `fail_fast` always runs them one at a time.
        progress: Told of each probe as a unit of step `smoke`, as it starts and ends.
    """

    def __init__(
        self, port: ExecutionPort, readers: Fanout[ExecutionPort] | None = None, *, progress: Progress = NO_PROGRESS
    ) -> None:
        self._port = port
        self._readers = readers
        self._progress = progress

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
        # Run one at a time only under `fail_fast`, so the first failure ends the run here.
        for artifact in rendered:
            if failures:
                if artifact.smoke:
                    skipped.append(_skipped(artifact, "--fail-fast stopped the suite at an earlier failure"))
                continue
            for probe in artifact.smoke:
                attempted.append(probe)
                error = self._probe_error(self._port, (artifact, probe))
                if error is not None:
                    failures.append(_probe_failure(artifact, probe, error))
                    break
        tally = (D("SST-APL006", count=len(failures)),) if failures else ()
        return SmokeResult(tuple(attempted), DiagnosticBag((*failures, *tally, *skipped)))

    def _run_all(self, rendered: tuple[RenderedArtifact, ...]) -> SmokeResult:
        """Run every probe, concurrently when given sessions, and report failures in probe order."""
        probes = tuple((artifact, probe) for artifact in rendered for probe in artifact.smoke)
        errors = (self._readers or Fanout(self._port)).map(self._probe_error, probes)
        failures = tuple(
            _probe_failure(artifact, probe, error)
            for (artifact, probe), error in zip(probes, errors, strict=True)
            if error is not None
        )
        tally = (D("SST-APL006", count=len(failures)),) if failures else ()
        return SmokeResult(tuple(probe for _, probe in probes), DiagnosticBag((*failures, *tally)))

    def _probe_error(self, port: ExecutionPort, item: tuple[RenderedArtifact, SmokeProbe]) -> str | None:
        """Run one probe, reporting it as a unit; why it failed, or None when it answered."""
        key = item[1].key
        self._progress.unit_started("smoke", key, "probing")
        try:
            port.query(item[1].sql)
        except SnowflakePortError as exc:
            self._progress.unit_finished("smoke", key, ok=False, status="failed")
            return str(exc)
        self._progress.unit_finished("smoke", key, ok=True, status="answered")
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
        phases: Times `smoke_check` (state and markers) and `smoke` (the probes, only when they run).
        progress: Told of each probe as a unit, as `RunSmokeSuite` tells it.
    """

    def __init__(
        self,
        port: SmokePort,
        state_store: StateStore,
        readers: Fanout[SmokePort] | None = None,
        *,
        phases: PhaseTimer = NO_PHASES,
        progress: Progress = NO_PROGRESS,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._readers = readers
        self._phases = phases
        self._progress = progress

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
        2. Render each artifact as apply published it for `manifest`.
        3. Check each artifact with probes: state records it at the fingerprint and target it
           compiles to, and its live marker is the one that entry recorded, from whichever
           manifest last published it. Composite artifacts carry no marker and have no probe,
           so they are not asked for one.
        4. Check every public metric has a probe.
        5. Run the probes only when nothing was reported; otherwise probe nothing.

        Args:
            manifest: The manifest the project compiles to, which `sst compile` wrote.

        Diagnostics:
            SST-APL036, SST-APL037, SST-APL038, SST-APL012: an object with probes is not published
                on the target, nothing the suite probes is, one is published from an older
                compile, or one changed since it was applied; as `unpublished_findings` says.
                None is reported when state could not be read, which says so itself.
            Those of `read_state`, any of which also keeps the probes from running, and the
            probes' own.
        """
        with self._phases.phase("smoke_check"):
            state, state_diagnostics = read_state(self._state_store, self._port, state_table=state_table, target=target)
            problems: list[Diagnostic] = list(state_diagnostics)
            published = {artifact.key: artifact for artifact in result.rendered_for_publish(manifest.manifest_id)}
            checked = () if state_diagnostics.has_errors else self._checked(result, state)
            problems.extend(unpublished_findings(checked, command="test"))
            problems.extend(unprobed_metrics(result))
        if problems:
            return SmokeResult((), DiagnosticBag(tuple(problems)), unpublished_count(checked))
        with self._phases.phase("smoke"):
            suite = RunSmokeSuite(self._port, self._readers, progress=self._progress)
            return suite.run(tuple(published.values()), fail_fast=fail_fast)

    def _checked(self, result: CompileResult, state: State) -> tuple[Checked, ...]:
        """Say how each artifact with probes is published, in compile order.

        The live marker is read only for an artifact whose state entry matches.
        """
        probed = tuple(item for item in result.rendered if item.smoke)
        recorded = {artifact.key: entry for artifact in probed if (entry := _recorded(artifact, state)) is not None}
        owned = tuple(artifact for artifact in probed if artifact.key in recorded)
        markers = dict(
            zip(
                (artifact.key for artifact in owned),
                (self._readers or Fanout(self._port)).map(
                    lambda port, artifact: port.describe_marker(artifact.target, artifact.object_type), owned
                ),
                strict=True,
            )
        )
        return tuple(
            Checked(artifact.key, artifact.target.sql, _publication(artifact, state, markers)) for artifact in probed
        )


def _publication(artifact: RenderedArtifact, state: State, markers: dict[str, OwnershipMarker | None]) -> Publication:
    """Say how one artifact is published, from its state entry and, when that matches, its marker."""
    entry = _recorded(artifact, state)
    if entry is None:
        return Publication.ABSENT if artifact.key not in state.applied else Publication.OLDER
    if markers.get(artifact.key) != OwnershipMarker(entry.manifest_id, artifact.fingerprint):
        return Publication.DRIFTED
    return Publication.PUBLISHED


def _recorded(artifact: RenderedArtifact, state: State) -> AppliedEntry | None:
    """Return the entry recording the artifact at its fingerprint and target; None when there is none.

    The manifest the entry names is the one that last published the artifact, which need not
    be the one being tested: an artifact no later apply changed keeps its entry as it was.
    """
    entry = state.applied.get(artifact.key)
    if (
        entry is not None
        and entry.fingerprint == artifact.fingerprint
        and QualifiedName.parse(entry.qualified_name).folded == artifact.target.folded
    ):
        return entry
    return None
