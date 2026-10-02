"""Execute rendered smoke probes separately from publication.

`RunSmokeSuite` runs probes. `SmokePublished` is the smoke test suite's use case: it first
proves SST owns each object it would probe, from authoritative state and the object's live
ownership marker, and runs the probes only once it does.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort, SnowflakePortError, StateStore
from snowflake_semantic_tools.domain.state import Manifest, State


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
    """Run each rendered artifact's smoke probes, which read and never write."""

    def __init__(self, port: SnowflakePort) -> None:
        self._port = port

    def run(self, rendered: tuple[RenderedArtifact, ...], *, fail_fast: bool = False) -> SmokeResult:
        """Run every probe, in artifact order; with `fail_fast`, stop at the first that fails.

        Diagnostics:
            SST-APL100: a probe failed.
            SST-APL006: after the failures, how many probes failed.
        """
        attempted: list[SmokeProbe] = []
        diagnostics = []
        for artifact in rendered:
            for probe in artifact.smoke:
                attempted.append(probe)
                try:
                    self._port.query(probe.sql)
                except SnowflakePortError as exc:
                    diagnostics.append(D("SST-APL100", artifact=probe.key, detail=str(exc), subject=artifact.key))
                    if fail_fast:
                        break
            if diagnostics and fail_fast:
                break
        if diagnostics:
            diagnostics.append(D("SST-APL006", count=len(diagnostics)))
        return SmokeResult(tuple(attempted), DiagnosticBag(diagnostics))


class SmokePublished:
    """Probe the published objects of a compiled project, once SST is proven to own each one.

    Ownership is proven from authoritative state and each object's live ownership marker, so
    no probe runs against an object some other run, or someone else, published.
    """

    def __init__(self, port: SnowflakePort, state_store: StateStore) -> None:
        self._port = port
        self._state_store = state_store

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
        5. Run the probes only when nothing was reported; otherwise probe nothing.

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
        if problems:
            return SmokeResult((), DiagnosticBag(tuple(problems)))
        return RunSmokeSuite(self._port).run(tuple(published.values()), fail_fast=fail_fast)

    def _unowned(self, result: CompileResult, manifest: Manifest, state: State) -> list[Diagnostic]:
        """Report each artifact with probes whose state entry or live marker is not apply's, in compile order.

        The live marker is read only for an artifact whose state entry matches.
        """
        unowned: list[Diagnostic] = []
        for artifact in (item for item in result.rendered if item.smoke):
            entry = state.applied.get(artifact.key)
            expected = OwnershipMarker(manifest.manifest_id, artifact.fingerprint)
            if (
                entry is None
                or entry.fingerprint != artifact.fingerprint
                or entry.manifest_id != manifest.manifest_id
                or QualifiedName.parse(entry.qualified_name).folded != artifact.target.folded
                or self._port.describe_marker(artifact.target) != expected
            ):
                unowned.append(D("SST-APL012", artifact=artifact.key, value=artifact.target.sql))
        return unowned
