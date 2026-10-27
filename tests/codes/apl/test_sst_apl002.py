"""SST-APL002: a change was skipped because one it depends on failed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, OutcomeStatus, RenderedArtifact
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake, failed


def plan_of_two() -> tuple[RenderedArtifact, RenderedArtifact]:
    first = rendered("A")
    return first, rendered("B", depends_on=(first.key,))


def test_sst_apl002_fires() -> None:
    first, second = plan_of_two()
    port = FakeSnowflake()
    port.execute_results = [failed("denied")]
    diagnostic = only(apply_plan(changeset(change(first), change(second)), port).diagnostics, "SST-APL002")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:b skipped: semantic_view:a failed"
    assert diagnostic.context["artifact"] == "semantic_view:b"


def test_sst_apl002_silent() -> None:
    first, second = plan_of_two()
    assert "SST-APL002" not in codes(apply_plan(changeset(change(first), change(second))).diagnostics)


def test_sst_apl002_names_the_failure_a_skipped_dependency_descends_from_and_spares_a_noop() -> None:
    # A fails; B, which the plan had nothing to do for, is skipped without a warning; C, which
    # depends on B, is skipped naming A, the change that failed, not B, which only was skipped.
    first = rendered("A")
    unchanged = rendered("B", depends_on=(first.key,))
    third = rendered("C", depends_on=(unchanged.key,))
    port = FakeSnowflake()
    port.execute_results = [failed("denied")]
    result = apply_plan(changeset(change(first), change(unchanged, Action.NOOP), change(third)), port)
    assert [item.message for item in result.diagnostics if item.code == "SST-APL002"] == [
        "semantic_view:c skipped: semantic_view:a failed"
    ]
    assert [(item.key, item.status) for item in result.outcomes][1:] == [
        ("semantic_view:b", OutcomeStatus.SKIPPED),
        ("semantic_view:c", OutcomeStatus.SKIPPED),
    ]
