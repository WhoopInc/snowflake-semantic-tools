"""SST-PLN005: the artifacts' dependencies form a cycle, so nothing can be ordered."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import plan, view


def test_sst_pln005_fires() -> None:
    first = view("a", depends_on=("semantic_view:b",))
    second = view("b", depends_on=("semantic_view:a",))
    planned = plan((first, second))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN005")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "artifact dependency cycle: semantic_view:a -> semantic_view:b -> semantic_view:a"
    assert planned.changes == () and planned.plan_id == ""


def test_sst_pln005_silent() -> None:
    planned = plan((view("a", depends_on=("semantic_view:b",)), view("b")))
    assert coded(planned.diagnostics, "SST-PLN005") == []
    assert [change.key for change in planned.changes] == ["semantic_view:b", "semantic_view:a"]
