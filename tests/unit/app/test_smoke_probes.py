"""Smoke probes for window metrics, unbuildable probes, and what --fail-fast leaves unprobed."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.smoke import RunSmokeSuite, SmokePublished
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, ProbeKind
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SortKey, Window
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.compile_builders import view
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs, dev_target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored


def _with_metric(metric: Metric) -> CompileResult:
    sales = replace(view("SALES"), metrics=(metric,))
    return CompileProject(InMemoryProjectInputs(views=SemanticViewProject((sales,)))).run()


def test_a_window_metric_probe_requests_each_dimension_its_window_excludes() -> None:
    window = Window(partition_excluding=(authored("T.C"),))
    result = _with_metric(Metric("RUNNING", authored("SUM(T.C)"), table="T", window=window))
    [probe] = [item for item in result.rendered[0].smoke if item.kind is ProbeKind.METRIC]
    assert str(probe.sql) == (
        "SELECT SV.RUNNING FROM SEMANTIC_VIEW(DB.SCH.SALES METRICS T.RUNNING DIMENSIONS T.C) AS SV LIMIT 1"
    )


def test_a_published_view_with_an_unbuildable_metric_probe_runs_no_probe() -> None:
    window = Window(partition_excluding=(authored("DATE_TRUNC('month', T.C)"),))
    result = _with_metric(Metric("RUNNING", authored("SUM(T.C)"), table="T", window=window))
    assert isinstance(result.compiled[0], CompiledView)
    manifest = manifest_for(result, EMPTY_SOURCES)
    port = FakeSnowflake()
    port.remote_state = MappingProxyType(
        {
            artifact.key: AppliedEntry(
                artifact.fingerprint,
                artifact.target.sql,
                "now",
                "run",
                "applied",
                artifact.fingerprint,
                manifest.manifest_id,
            )
            for artifact in result.rendered
        }
    )
    port.markers = {
        artifact.target.sql: OwnershipMarker(manifest.manifest_id, artifact.fingerprint) for artifact in result.rendered
    }
    outcome = SmokePublished(port, InMemoryStateStore()).run(
        result, manifest, target=dev_target(), state_table=QualifiedName.parse("DB.SCH.SST_STATE")
    )
    assert [item.code for item in outcome.diagnostics] == ["SST-PLN020"] and outcome.attempted == ()


def test_fail_fast_reports_every_later_artifact_with_probes_as_skipped() -> None:
    first = _with_metric(Metric("A", authored("SUM(T.C)"), table="T")).rendered[0]
    other = replace(first, key="semantic_view:other")
    unprobed = replace(first, key="semantic_view:bare", smoke=())
    port = FakeSnowflake()
    port.fail("query", SnowflakePortError("broken"))
    result = RunSmokeSuite(port).run((first, unprobed, other), fail_fast=True)
    assert [(item.code, item.subject) for item in result.diagnostics] == [
        ("SST-APL100", first.key),
        ("SST-APL006", None),
        ("SST-PLN101", other.key),
    ]


def test_a_window_metric_probe_requests_the_dimensions_its_window_partitions_and_orders_by() -> None:
    window = Window(
        partition_by=(authored("T.LOCATION_ID"),),
        order_by=(SortKey(authored("T.ORDERED_DATE"), descending=True), SortKey(authored("t.location_id"))),
    )
    result = _with_metric(
        Metric("RUNNING", authored("SUM(T.C) OVER (PARTITION BY EXCLUDING T.C)"), table="T", window=window)
    )
    [probe] = [item for item in result.rendered[0].smoke if item.kind is ProbeKind.METRIC]
    assert str(probe.sql) == (
        "SELECT SV.RUNNING FROM SEMANTIC_VIEW(DB.SCH.SALES METRICS T.RUNNING "
        "DIMENSIONS T.C, T.LOCATION_ID, T.ORDERED_DATE) AS SV LIMIT 1"
    )


def test_a_window_ordered_by_an_expression_leaves_its_metric_unprobed() -> None:
    window = Window(order_by=(SortKey(authored("DATE_TRUNC('month', T.ORDERED_DATE)")),))
    result = _with_metric(Metric("RUNNING", authored("SUM(T.C)"), table="T", window=window))
    assert [item for item in result.rendered[0].smoke if item.kind is ProbeKind.METRIC] == []
