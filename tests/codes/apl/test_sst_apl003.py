"""SST-APL003: apply refuses a plan carrying a blocked artifact it does not skip."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOptions, ChangeSet, FailurePolicy
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.diagnostic_filters import codes, only


def blocked_plan(*, error: bool = False) -> ChangeSet:
    carried = D("SST-REF001", model="x") if error else D("SST-PLN014", artifact="x", detail="y")
    blocked = replace(change(rendered(), Action.BLOCKED), diagnostics=DiagnosticBag((carried,)))
    return changeset(blocked)


def test_sst_apl003_fires() -> None:
    # Fail-fast refuses any blocked change; without it, one carrying an error of its own.
    for plan, options in (
        (blocked_plan(), ApplyOptions(on_failure=FailurePolicy.STOP_ALL)),
        (blocked_plan(error=True), ApplyOptions()),
    ):
        result = apply_plan(plan, options=options)
        diagnostic = only(result.diagnostics, "SST-APL003")
        assert diagnostic.severity is Severity.ERROR
        assert diagnostic.message == "semantic_view:v is BLOCKED by 1 errors"
        assert diagnostic.context["artifact"] == "semantic_view:v"
        assert not result.state_written


def test_sst_apl003_silent() -> None:
    # Without fail-fast a blocked change carrying only warnings is skipped, as CONTINUE skips any.
    for options in (ApplyOptions(), ApplyOptions(on_failure=FailurePolicy.CONTINUE)):
        result = apply_plan(blocked_plan(), options=options)
        assert "SST-APL003" not in codes(result.diagnostics)
