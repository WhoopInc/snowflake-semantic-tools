"""What a model comparison notices beyond its report: its cost's state, and whom it leaves out.

These are info: none of them fails a comparison. Each is reported once per eval, but the excluded
candidates, one per model; a refreshed report reports them again from its new cost.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.evaluate.compare import (
    PENDING,
    UNAVAILABLE,
    EvalComparison,
    cost_states,
    unavailable_reason,
)


def comparison_notices(evaluation: EvalComparison, *, path: str) -> tuple[Diagnostic, ...]:
    """Report one eval's cost states and excluded candidates, in that order.

    Args:
        path: Where the report is written, which a pending cost's notice names to refresh.

    Diagnostics:
        SST-VAL773: some model's cost is pending, since its usage has not landed yet.
        SST-VAL774: some model's cost is unavailable; the reason is the first model's.
        SST-VAL775: a candidate is in preview, internal or deprecated, so no verdict names it.
        SST-VAL776: the eval is a CoWork profile's, whose cost is its router's alone.
    """
    folded = evaluation.eval.casefold()
    subject = f"eval:{folded}" if evaluation.kind == "agent" else f"eval:cowork_profile:{folded}"
    states = cost_states(evaluation)
    found: list[Diagnostic] = []
    if states[PENDING]:
        found.append(D("SST-VAL773", artifact=evaluation.eval, count=len(states[PENDING]), path=path, subject=subject))
    if states[UNAVAILABLE]:
        reason = unavailable_reason(evaluation)
        found.append(
            D(
                "SST-VAL774",
                artifact=evaluation.eval,
                count=len(states[UNAVAILABLE]),
                reason=reason,
                path=path,
                subject=subject,
            )
        )
    found.extend(
        D("SST-VAL775", artifact=evaluation.eval, model=model, kind=lifecycle, subject=subject)
        for model, lifecycle in evaluation.verdict.excluded
    )
    if evaluation.kind == "cowork_profile":
        found.append(D("SST-VAL776", artifact=evaluation.eval, subject=subject))
    return tuple(found)
