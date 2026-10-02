"""SST-PLN101: with --fail-fast, an artifact's smoke probes did not run once an earlier one failed."""

from __future__ import annotations

from snowflake_semantic_tools.app.smoke import RunSmokeSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.artifact_builders import rendered
from tests.helpers.sql_values import statement


def _view(name: str) -> RenderedArtifact:
    probe = SmokeProbe(f"semantic_view:{name}:view", ProbeKind.VIEW, statement("select 1"))
    return RenderedArtifact.create(
        key=f"semantic_view:{name}",
        artifact_type="semantic_view",
        target=rendered(name.upper()).target,
        ddl=statement("ddl"),
        smoke=(probe,),
    )


def _failing() -> InMemorySnowflake:
    port = InMemorySnowflake()
    port.query_error = SnowflakePortError("broken")
    return port


def test_sst_pln101_fires() -> None:
    result = RunSmokeSuite(_failing()).run((_view("a"), _view("b")), fail_fast=True)
    [skipped] = [item for item in result.diagnostics if item.code == "SST-PLN101"]
    assert skipped.severity is Severity.INFO
    assert (
        skipped.message == "semantic_view:b: smoke check skipped (--fail-fast stopped the suite at an earlier failure)"
    )


def test_sst_pln101_silent() -> None:
    result = RunSmokeSuite(_failing()).run((_view("a"), _view("b")))
    assert "SST-PLN101" not in [item.code for item in result.diagnostics] and len(result.attempted) == 2
