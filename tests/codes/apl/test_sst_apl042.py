"""SST-APL042: without fail-fast, apply skips a blocked change, applies the rest, and exits 1."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity, resolve_severities
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ChangeSet,
    FailurePolicy,
    OutcomeStatus,
)
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.diagnostic_filters import codes, only


def _plan(*, strict: bool = False) -> ChangeSet:
    blocked_view = rendered("BLOCKED_V")
    out_of_band = D("SST-PLN014", subject=blocked_view.key, artifact=blocked_view.key, detail="its comment was changed")
    blocked = replace(change(blocked_view, Action.BLOCKED), diagnostics=DiagnosticBag((out_of_band,)))
    plan = changeset(blocked, change(rendered("HEALTHY_V")))
    if strict:
        promoted, _ = resolve_severities(DiagnosticBag((out_of_band,)), strict=True)
        plan = replace(plan, diagnostics=promoted)
    return plan


def test_sst_apl042_fires() -> None:
    for strict in (False, True):
        result = apply_plan(_plan(strict=strict))
        diagnostic = only(result.diagnostics, "SST-APL042")
        assert diagnostic.severity is Severity.WARNING
        assert diagnostic.message == (
            "semantic_view:blocked_v was skipped: the plan blocked it, so apply goes on with the rest and exits 1"
        )
        assert diagnostic.subject == "semantic_view:blocked_v"
        statuses = {outcome.key: outcome.status for outcome in result.outcomes}
        # The healthy change applies and state records it, even when strict made the warning an error.
        assert statuses == {
            "semantic_view:blocked_v": OutcomeStatus.SKIPPED,
            "semantic_view:healthy_v": OutcomeStatus.APPLIED,
        }
        assert result.state_written
        assert not result.success


def test_sst_apl042_silent() -> None:
    # Fail-fast refuses the whole plan instead, and a plan with nothing blocked skips nothing.
    refused = apply_plan(_plan(), options=ApplyOptions(on_failure=FailurePolicy.STOP_ALL))
    assert "SST-APL042" not in codes(refused.diagnostics) and "SST-APL003" in codes(refused.diagnostics)
    healthy = apply_plan(changeset(change(rendered("HEALTHY_V"))))
    assert "SST-APL042" not in codes(healthy.diagnostics) and healthy.success
