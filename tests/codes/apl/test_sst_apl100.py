"""SST-APL100: a published artifact's smoke probe failed to run."""

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
    # A metric's failed probe is SST-PLN100's; this code reports any other probe.
    probe = SmokeProbe("verified_query:v.top_customers", ProbeKind.VERIFIED_QUERY, sql("SELECT 1"))
    return replace(rendered(), smoke=(probe,))


def test_sst_apl100_fires() -> None:
    port = InMemorySnowflake()
    port.query_error = SnowflakePortError("invalid identifier 'REVENUE'")
    [diagnostic, _] = RunSmokeSuite(port).run((probed(),)).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL100", Severity.ERROR)
    assert diagnostic.message == "verified_query:v.top_customers: smoke probe failed: invalid identifier 'REVENUE'"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_apl100_silent() -> None:
    assert "SST-APL100" not in [item.code for item in RunSmokeSuite(InMemorySnowflake()).run((probed(),)).diagnostics]
