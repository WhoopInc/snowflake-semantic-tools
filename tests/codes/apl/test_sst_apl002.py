"""SST-APL002: a change was skipped because one it depends on failed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from tests.helpers.app_ports import InMemorySnowflake, failed
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, rendered


def plan_of_two() -> tuple[RenderedArtifact, RenderedArtifact]:
    first = rendered("A")
    return first, rendered("B", depends_on=(first.key,))


def test_sst_apl002_fires() -> None:
    first, second = plan_of_two()
    port = InMemorySnowflake()
    port.execute_results = [failed("denied")]
    diagnostic = only(apply_plan(changeset(change(first), change(second)), port), "SST-APL002")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:b skipped: semantic_view:a failed"
    assert diagnostic.context["artifact"] == "semantic_view:b"


def test_sst_apl002_silent() -> None:
    first, second = plan_of_two()
    assert "SST-APL002" not in codes(apply_plan(changeset(change(first), change(second))))
