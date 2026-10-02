"""SST-APL003: apply refuses a plan carrying a blocked artifact."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOptions, ChangeSet, FailurePolicy
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, rendered


def blocked_plan() -> ChangeSet:
    blocked = replace(
        change(rendered(), Action.BLOCKED), diagnostics=DiagnosticBag((D("SST-PLN024", artifact="x", value="y"),))
    )
    return changeset(blocked)


def test_sst_apl003_fires() -> None:
    result = apply_plan(blocked_plan())
    diagnostic = only(result, "SST-APL003")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v is BLOCKED by 1 errors"
    assert diagnostic.context["artifact"] == "semantic_view:v"
    assert not result.state_written


def test_sst_apl003_silent() -> None:
    # CONTINUE runs what it can and skips the blocked change instead of refusing the plan.
    result = apply_plan(blocked_plan(), options=ApplyOptions(on_failure=FailurePolicy.CONTINUE))
    assert "SST-APL003" not in codes(result)
