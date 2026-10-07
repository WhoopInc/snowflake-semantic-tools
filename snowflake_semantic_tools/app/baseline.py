"""Hold a run's diagnostics to the committed baseline, and date the baseline's own edits.

A baselined diagnostic never blocks: a run whose exit follows its diagnostics, and whose every
blocking diagnostic the baseline holds, passes. A baseline past its expiry blocks instead. Today's
date is read from the `ClockPort`, so a test fixes it and nothing here reads the wall clock.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, match_baseline, stable_fingerprint
from snowflake_semantic_tools.domain.ports.clock import ClockPort

# A baseline expiring within this many days is reported as nearing expiry.
EXPIRY_WARNING_DAYS = 30


@dataclass(frozen=True, slots=True)
class BaselineGate:
    """What the baseline did to one run's result.

    Attributes:
        diagnostics: The notices about the baseline itself, then the run's own diagnostics.
        baselined: The stable fingerprints of the diagnostics the baseline holds.
        forgiven: Whether the run has a blocking diagnostic and the baseline holds every one, so a
            run whose exit follows its diagnostics passes after all.
        expired: Whether a notice about the baseline blocks, so a passing run fails.
    """

    diagnostics: DiagnosticBag
    baselined: frozenset[str]
    forgiven: bool
    expired: bool


def gate_baseline(diagnostics: DiagnosticBag, baseline: Baseline, *, clock: ClockPort) -> BaselineGate:
    """Match `diagnostics` against `baseline` as of the clock's today, and say what that decides.

    Diagnostics:
        Those of `match_baseline`: SST-CFG039, SST-CFG035, SST-INT009.
    """
    today = clock_today(clock)
    match = match_baseline(
        diagnostics,
        baseline,
        today=today.isoformat(),
        warn_from=days_after(today, EXPIRY_WARNING_DAYS),
    )
    errors = [item for item in diagnostics if item.blocks]
    return BaselineGate(
        DiagnosticBag((*match.notices, *diagnostics)),
        match.baselined,
        forgiven=bool(errors) and all(stable_fingerprint(item) in match.baselined for item in errors),
        expired=any(item.blocks for item in match.notices),
    )


def held_by_baseline(diagnostics: DiagnosticBag, baseline: Baseline | None, *, clock: ClockPort) -> frozenset[str]:
    """Return the stable fingerprints of the diagnostics `baseline` holds today; none without one.

    The same match the run's report applies, so a command that gates on its diagnostics before
    reporting them -- a plan refusing on a validation error -- holds the same ones.
    """
    if baseline is None:
        return frozenset()
    return gate_baseline(diagnostics, baseline, clock=clock).baselined


def blocking(diagnostics: DiagnosticBag, held: frozenset[str]) -> bool:
    """Report whether some diagnostic blocks and is not one `held` names."""
    return any(item.blocks and stable_fingerprint(item) not in held for item in diagnostics)


def clock_today(clock: ClockPort) -> date:
    """Return the clock's current date in UTC."""
    return date.fromisoformat(clock.now_iso()[:10])


def days_after(today: date, days: int) -> str:
    """Return the ISO date `days` after `today`."""
    return (today + timedelta(days=days)).isoformat()


def clock_stamp(clock: ClockPort) -> str:
    """Return the clock's current time in UTC to the second, as ISO 8601 text ending in `Z`."""
    return f"{clock.now_iso()[:19]}Z"
