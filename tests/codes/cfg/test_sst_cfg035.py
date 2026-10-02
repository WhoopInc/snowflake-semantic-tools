"""SST-CFG035: the baseline expires within 30 days."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry, match_baseline

BASELINE = Baseline(".sst/baseline.json", "2026-10-20", (BaselineEntry("0" * 16, "SST-VAL003"),))


def test_sst_cfg035_fires() -> None:
    [diagnostic] = match_baseline((), BASELINE, today="2026-10-02", warn_from="2026-11-01").notices
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG035", Severity.WARNING)
    assert diagnostic.message == "baseline expires on 2026-10-20; 1 entries remain"
    assert (diagnostic.subject, diagnostic.origin) == ("config:.sst/baseline.json", Origin(".sst/baseline.json"))


def test_sst_cfg035_silent() -> None:
    assert match_baseline((), BASELINE, today="2026-09-01", warn_from="2026-10-01").notices == ()
    assert match_baseline((), None, today="2026-10-02", warn_from="2026-11-01").notices == ()
