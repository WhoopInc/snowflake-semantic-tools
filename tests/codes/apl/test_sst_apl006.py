"""SST-APL006: the smoke suite ran probes that failed, so it is not a success."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.smoke import RunSmokeSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import sql
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.artifact_builders import rendered


def probed() -> RenderedArtifact:
    probe = SmokeProbe("semantic_view:v:view", ProbeKind.VIEW, sql("SELECT * FROM DB.SCHEMA.V LIMIT 1"))
    return replace(rendered(), smoke=(probe,))


def test_sst_apl006_fires() -> None:
    port = InMemorySnowflake()
    port.query_error = SnowflakePortError("Object 'DB.SCHEMA.V' does not exist")
    result = RunSmokeSuite(port).run((probed(),))
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL006"]
    assert (diagnostic.severity, diagnostic.message) == (Severity.ERROR, "1 published probes failed")
    assert not result.success


def test_sst_apl006_silent() -> None:
    result = RunSmokeSuite(InMemorySnowflake()).run((probed(),))
    assert [item.code for item in result.diagnostics] == [] and result.success
