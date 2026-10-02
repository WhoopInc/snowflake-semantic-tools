"""SST-CFG039: the baseline is past its expiry, so what it held blocks again."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    Baseline,
    BaselineEntry,
    match_baseline,
    stable_fingerprint,
)

WARNING = D("SST-CFG018", group="platform", name="docs", subject="tool_group:platform")


def _baseline(expires_on: str) -> Baseline:
    return Baseline(".sst/baseline.json", expires_on, (BaselineEntry(stable_fingerprint(WARNING), "SST-CFG018"),))


def test_sst_cfg039_fires() -> None:
    match = match_baseline((WARNING,), _baseline("2026-10-01"), today="2026-10-02", warn_from="2026-11-01")
    [diagnostic] = match.notices
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG039", Severity.ERROR)
    assert diagnostic.message == "baseline expired on 2026-10-01; 1 entries resume blocking"
    assert match.baselined == frozenset()


def test_sst_cfg039_silent() -> None:
    match = match_baseline((WARNING,), _baseline("2027-03-01"), today="2026-10-02", warn_from="2026-11-01")
    assert (match.notices, match.baselined) == ((), frozenset((stable_fingerprint(WARNING),)))
