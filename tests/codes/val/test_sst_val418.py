"""SST-VAL418: an expression or verified query would not compile as the one expression SST splices in.

Offline, the loader's guard reports a metric whose expression ends the statement; connected,
Snowflake's own compile errors are reported under the same code.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.projects import load_project

MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "reference_project_manifest.json"
METRICS = "semantic_models/metrics/metrics.yml"
ORDER_COUNT = "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"


def test_sst_val418_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    path = project / METRICS
    path.write_text(path.read_text().replace(ORDER_COUNT, f"{ORDER_COUNT}); DROP TABLE x; --", 1))
    loaded = load_project(project, manifest_path=MANIFEST)
    found = [item for item in loaded.diagnostics if item.code == "SST-VAL418"]
    assert found and all(item.severity is Severity.ERROR for item in found)
    assert found[0].message == (
        "metric 'order_count': expression failed to compile: ')' closes nothing at '); DROP TABLE x; --', "
        "so SST will not send it to Snowflake"
    )
    assert found[0].subject == "metric:order_count"
    assert all("DROP TABLE" not in str(view.metrics) for view in loaded.views)


def test_sst_val418_silent(tmp_path: Path) -> None:
    diagnostics = load_project(project_copy(tmp_path), manifest_path=MANIFEST).diagnostics
    assert "SST-VAL418" not in [item.code for item in diagnostics]
