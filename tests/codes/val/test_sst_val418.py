"""SST-VAL418: an expression or verified query would not compile as the one expression SST splices in.

Offline, the loader's guard reports a metric whose expression ends the statement; connected,
Snowflake's own compile errors are reported under the same code, among them an argument type
it refuses for a column typed as the target describes it.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table
from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.diagnostic_filters import coded
from tests.helpers.projects import load_project
from tests.helpers.reference_project import project_copy
from tests.helpers.sql_values import authored
from tests.helpers.val_codes import TypedWarehouse

MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "reference_project_manifest.json"
METRICS = "semantic_models/metrics/metrics.yml"
ORDER_COUNT = "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"
# What Snowflake answers for SUM over a timestamp, once the reference is typed.
TYPE_ERROR = "SQL compilation error: Invalid argument types for function 'SUM': (TIMESTAMP_NTZ(9))"


def _connected(metric: str) -> list[Diagnostic]:
    views = tuple(
        SemanticView(
            f"DB.S.{name}", (Table("ORDERS", "DB.S.ORDERS"),), metrics=(Metric("M", authored(metric), "ORDERS"),)
        )
        for name in ("MENU", "SALES")
    )
    port = TypedWarehouse(
        {"DB.S.ORDERS": (("ORDERED_AT", "TIMESTAMP_NTZ(9)"),)}, {'SUM("ORDERS.ORDERED_AT")': TYPE_ERROR}
    )
    compiled = CompileResult(tuple(CompiledView(view, render(view)) for view in views))
    return coded(ValidateArtifacts(port).run(compiled, strict=False, connected=True).diagnostics, "SST-VAL418")


def test_sst_val418_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    path = project / METRICS
    path.write_text(path.read_text().replace(ORDER_COUNT, f"{ORDER_COUNT}); DROP TABLE x; --", 1))
    loaded = load_project(project, manifest_path=MANIFEST)
    found = [item for item in loaded.diagnostics if item.code == "SST-VAL418"]
    assert found and all(item.severity is Severity.ERROR for item in found)
    assert found[0].message == (
        "metric 'order_count' is not valid SQL: ')' closes nothing at '); DROP TABLE x; --', "
        "so SST will not send it to Snowflake"
    )
    assert found[0].subject == "metric:order_count"
    assert all("DROP TABLE" not in str(view.metrics) for view in loaded.views)


def test_sst_val418_once_for_a_metric_two_views_hold(tmp_path: Path) -> None:
    clean = load_project(project_copy(tmp_path / "clean"), manifest_path=MANIFEST)
    holders = [view for view in clean.views if any(metric.name == "ORDER_COUNT" for metric in view.metrics)]
    assert len(holders) >= 2
    project = project_copy(tmp_path / "broken")
    path = project / METRICS
    path.write_text(path.read_text().replace(ORDER_COUNT, f"{ORDER_COUNT}); DROP TABLE x; --", 1))
    diagnostics = load_project(project, manifest_path=MANIFEST).diagnostics
    assert [item.subject for item in diagnostics if item.code == "SST-VAL418"] == ["metric:order_count"]


def test_sst_val418_silent(tmp_path: Path) -> None:
    diagnostics = load_project(project_copy(tmp_path), manifest_path=MANIFEST).diagnostics
    assert "SST-VAL418" not in [item.code for item in diagnostics]


def test_sst_val418_fires_connected_once_for_an_argument_type_two_views_refuse() -> None:
    [found] = _connected("SUM(ORDERS.ORDERED_AT)")
    assert found.severity is Severity.ERROR
    assert found.message == f"metric 'ORDERS.M' is not valid SQL: {TYPE_ERROR} (in menu, sales)"
    assert found.subject == "semantic_view:menu"


def test_sst_val418_silent_connected_for_an_argument_type_snowflake_takes() -> None:
    assert _connected("MAX(ORDERS.ORDERED_AT)") == []
