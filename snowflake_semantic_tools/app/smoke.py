"""Execute rendered smoke probes separately from publication."""

from __future__ import annotations

from dataclasses import dataclass

from ..domain.model.diagnostic import D, DiagnosticBag
from ..domain.model.lifecycle import RenderedArtifact, SmokeProbe
from ..domain.ports.snowflake import SnowflakePort, SnowflakePortError


@dataclass(frozen=True, slots=True)
class SmokeResult:
    attempted: tuple[SmokeProbe, ...]
    diagnostics: DiagnosticBag

    @property
    def success(self) -> bool:
        return not self.diagnostics.has_errors


class RunSmokeSuite:
    def __init__(self, port: SnowflakePort) -> None:
        self._port = port

    def run(self, rendered: tuple[RenderedArtifact, ...], *, fail_fast: bool = False) -> SmokeResult:
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
