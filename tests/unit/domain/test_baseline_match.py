"""The baseline the runner applies, and the severity decisions the diagnostics module owns."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Origin, Severity, override_refusal
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    Baseline,
    BaselineEntry,
    baselinable,
    match_baseline,
    stable_fingerprint,
)

TODAY, WARN_FROM = "2026-01-01", "2026-01-31"


def test_an_override_respects_the_demotion_floor() -> None:
    assert override_refusal("SST-REG001", Severity.WARNING) == "SST-REG001 is non-demotable"
    assert override_refusal("SST-CFG001", Severity.INFO) == "an error is demoted no lower than warning"
    assert override_refusal("SST-DIS200", Severity.ERROR) == "an info code is promoted no higher than warning"
    assert override_refusal("SST-CFG003", Severity.ERROR) is None


def test_informational_follows_the_resolved_severity() -> None:
    info = D("SST-DIS200", path="x.yml", type="metric")
    assert info.informational and not D("SST-CFG003", key="k").informational


def test_a_stable_fingerprint_ignores_the_line_and_keys_on_the_artifact_and_context() -> None:
    first = D("SST-CFG003", key="k", origin=Origin("sst_config.yml", 3), subject="config:k")
    moved = D("SST-CFG003", key="k", origin=Origin("sst_config.yml", 9), subject="config:k")
    renamed = D("SST-CFG003", key="k", origin=Origin("sst_config.yml", 3), subject="config:other")
    bare = D("SST-CFG003", key="k")
    assert stable_fingerprint(first) == stable_fingerprint(moved) != stable_fingerprint(renamed)
    assert len(stable_fingerprint(bare)) == 16


def test_only_a_demotable_code_below_error_is_baselinable() -> None:
    assert baselinable("SST-CFG003") and not baselinable("SST-CFG001") and not baselinable("SST-NOPE999")


def test_a_baseline_holds_what_it_records_and_reports_its_expiry() -> None:
    warning = D("SST-CFG003", key="k")
    entries = (BaselineEntry(stable_fingerprint(warning), "SST-CFG003"), BaselineEntry("ffff", "SST-CFG001"))
    assert match_baseline((warning,), None, today=TODAY, warn_from=WARN_FROM).baselined == frozenset()
    current = match_baseline((warning,), Baseline("b.json", "2030-01-01", entries), today=TODAY, warn_from=WARN_FROM)
    assert (current.baselined, current.notices) == (frozenset((stable_fingerprint(warning),)), ())
    soon = match_baseline((warning,), Baseline("b.json", "2026-01-15", entries), today=TODAY, warn_from=WARN_FROM)
    assert [item.code for item in soon.notices] == ["SST-CFG035"] and soon.baselined
    expired = match_baseline((warning,), Baseline("b.json", "2025-12-31", entries), today=TODAY, warn_from=WARN_FROM)
    assert [(item.code, item.subject) for item in expired.notices] == [("SST-CFG039", "config:b.json")]
    assert expired.baselined == frozenset()
