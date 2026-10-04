"""SST-REF900: a chain of `metric()` references nests deeper than the resolver follows."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.depth import MAX_METRIC_DEPTH
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import added, load

FILE = "semantic_models/metrics/chain.yml"


def _chain(hops: int) -> str:
    """Metrics `chain_0` to `chain_<hops>`, each referencing the next; the last counts orders."""
    links = "".join(
        f"  - name: chain_{index}\n    tables: [orders]\n    description: Link {index}.\n"
        f"    expr: \"{{{{ metric('chain_{index + 1}') }}}}\"\n"
        for index in range(hops)
    )
    end = (
        f"  - name: chain_{hops}\n    tables: [orders]\n    description: End.\n"
        "    expr: \"COUNT({{ ref('orders', 'order_id') }})\"\n"
    )
    return "snowflake_metrics:\n" + links + end


def test_sst_ref900_fires(tmp_path: Path) -> None:
    project = load(added(tmp_path, FILE, _chain(MAX_METRIC_DEPTH + 1)))
    [diagnostic] = coded(project.diagnostics, "SST-REF900")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "resolution depth 33 exceeded the limit 32"
    assert diagnostic.subject == "metric:chain_0"
    assert coded(project.diagnostics, "SST-REF005") == []


def test_sst_ref900_silent(tmp_path: Path) -> None:
    project = load(added(tmp_path, FILE, _chain(MAX_METRIC_DEPTH)))
    assert coded(project.diagnostics, "SST-REF900") == []
