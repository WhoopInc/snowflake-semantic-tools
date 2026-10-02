"""SST-MEM014: a member attached to a view is not in the view that was built from it.

The renderer renders every attached member, so this follows from no project: the fires
test holds a built reference view to an attachment that names a metric the build never
saw, and the silent test to one naming a metric the view holds.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.resolve.rendered import rendered_diagnostics
from tests.helpers.resolve_builders import coded, member, reference_fixture

SALES = "semantic_view:jaffle_sales"


def _dropped(tmp_path: Path, metric: str) -> list[Diagnostic]:
    """The SST-MEM014 findings when `metric` is attached to the built jaffle_sales view."""
    view = next(view for view in reference_fixture(tmp_path).views if view.fqn.endswith(".JAFFLE_SALES"))
    attached = member("metric", metric, ("orders",))
    return coded(rendered_diagnostics({SALES: view}, (attached,), {attached.key: (SALES,)}), "SST-MEM014")


def test_sst_mem014_fires(tmp_path: Path) -> None:
    [diagnostic] = _dropped(tmp_path, "ghost")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == "metric:ghost passed attachment for semantic_view:jaffle_sales and would be dropped at render"
    )
    assert diagnostic.subject == "metric:ghost"


def test_sst_mem014_silent(tmp_path: Path) -> None:
    assert _dropped(tmp_path, "order_count") == []
