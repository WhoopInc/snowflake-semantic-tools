"""SST-INT009: a baseline entry matched more than one diagnostic, so it baselines none of them."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    Baseline,
    BaselineEntry,
    match_baseline,
    stable_fingerprint,
)

DISTINCT = (D("SST-LOD003", file="a.yml"), D("SST-LOD003", file="b.yml"))


def _baseline(fingerprint: str) -> Baseline:
    return Baseline(".sst/baseline.json", "2030-01-01", (BaselineEntry(fingerprint, "SST-LOD003"),))


def test_sst_int009_fires() -> None:
    # The same warning reported twice has one fingerprint, so one entry matches both.
    repeated = (D("SST-LOD003", file="a.yml"), D("SST-LOD003", file="a.yml"))
    entry = stable_fingerprint(repeated[0])
    match = match_baseline(repeated, _baseline(entry), today="2026-01-01", warn_from="2026-01-31")
    [found] = match.notices
    assert (found.code, found.severity) == ("SST-INT009", Severity.ERROR)
    assert found.message == f"baseline entry {entry} matched 2 diagnostics"
    assert match.baselined == frozenset()


def test_sst_int009_silent() -> None:
    entry = stable_fingerprint(DISTINCT[0])
    match = match_baseline(DISTINCT, _baseline(entry), today="2026-01-01", warn_from="2026-01-31")
    assert match.notices == ()
    assert match.baselined == frozenset((entry,))
