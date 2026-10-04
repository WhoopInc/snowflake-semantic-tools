"""SST-PLN100: a metric's smoke query returned an error."""

from __future__ import annotations

from snowflake_semantic_tools.app.smoke import RunSmokeSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.artifact_builders import rendered
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement


def _view() -> RenderedArtifact:
    probe = SmokeProbe("metric:orders.revenue", ProbeKind.METRIC, statement("select revenue"))
    return RenderedArtifact.create(
        key="semantic_view:v",
        artifact_type="semantic_view",
        target=rendered().target,
        ddl=statement("ddl"),
        smoke=(probe,),
    )


def test_sst_pln100_fires() -> None:
    port = FakeSnowflake()
    port.fail("query", SnowflakePortError("SQL execution internal error"))
    failure, _ = RunSmokeSuite(port).run((_view(),)).diagnostics
    assert (failure.code, failure.severity) == ("SST-PLN100", Severity.ERROR)
    assert failure.message == (
        "semantic_view:v: smoke query for metric 'orders.revenue' failed: SQL execution internal error"
    )
    assert failure.subject == "metric:orders.revenue"


def test_sst_pln100_silent() -> None:
    assert RunSmokeSuite(FakeSnowflake()).run((_view(),)).diagnostics == ()
