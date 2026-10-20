"""SST-INT011: a blocking diagnostic the baseline does not hold may be one an unmatched entry baselined."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity, resolve_severities
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    Baseline,
    BaselineEntry,
    match_baseline,
    stable_fingerprint,
)


def _toolset(agent: str) -> Diagnostic:
    found = D("SST-VAL528", artifact=agent, name="crew", subject=f"agent:{agent}")
    [strict] = resolve_severities(DiagnosticBag((found,)), strict=True)[0]
    return strict


# What `sst baseline add SST-VAL528` recorded before the agent was renamed.
OLD = _toolset("concierge")
BASELINE = Baseline(
    ".sst/baseline.json", "2030-01-01", (BaselineEntry(stable_fingerprint(OLD), OLD.code, "agent:concierge"),)
)


def test_sst_int011_fires() -> None:
    match = match_baseline((_toolset("host"),), BASELINE, today="2026-01-01", warn_from="2026-01-31")
    [found] = match.hints
    assert (found.code, found.severity, found.subject) == ("SST-INT011", Severity.WARNING, "config:.sst/baseline.json")
    assert found.message == "SST-VAL528 on agent:host may be what was baselined under agent:concierge"
    assert match.baselined == frozenset()


def test_sst_int011_silent() -> None:
    match = match_baseline((OLD,), BASELINE, today="2026-01-01", warn_from="2026-01-31")
    assert (match.hints, match.notices) == ((), ())
    assert match.baselined == frozenset((stable_fingerprint(OLD),))
