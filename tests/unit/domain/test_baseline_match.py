"""The baseline the runner applies, and the severity decisions the diagnostics module owns."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.diagnostics import (
    ERROR_REGISTRY,
    D,
    Diagnostic,
    DiagnosticBag,
    Origin,
    Severity,
    _suggested,
    override_refusal,
    resolve_severities,
)
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    POSITION_KEYS,
    Baseline,
    BaselineEntry,
    baselinable,
    match_baseline,
    stable_fingerprint,
)
from snowflake_semantic_tools.domain.diagnostics.core import _placeholders

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


def test_no_baselinable_code_hashes_where_its_finding_sits() -> None:
    # A code's template may print its line or column; a baseline must not key on either. Its
    # suggestion may name a placeholder the template does not, which `D` requires too.
    for code in sorted(code for code in ERROR_REGISTRY if baselinable(code)):
        entry = ERROR_REGISTRY[code]
        context: dict[str, Any] = dict.fromkeys(_placeholders(entry.template) | _suggested(entry.suggestion), "x")
        here = D(code, **{**context, **dict.fromkeys(POSITION_KEYS, 3)})
        moved = D(code, **{**context, **dict.fromkeys(POSITION_KEYS, 9)})
        assert here.code == code and stable_fingerprint(here) == stable_fingerprint(moved), code


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


def test_expiry_is_judged_on_dates_across_a_month_boundary() -> None:
    warning = D("SST-CFG003", key="k")
    entries = (BaselineEntry(stable_fingerprint(warning), "SST-CFG003"),)

    def notices(expires_on: str) -> list[str]:
        found = match_baseline(
            (warning,), Baseline("b.json", expires_on, entries), today="2026-10-16", warn_from="2026-11-15"
        )
        return [item.code for item in found.notices]

    assert notices("2026-09-30") == ["SST-CFG039"]
    assert notices("2026-11-01") == ["SST-CFG035"]
    assert notices("2026-11-15") == ["SST-CFG035"]
    assert notices("2026-11-16") == []


def _toolset(agent: str) -> Diagnostic:
    return D("SST-VAL528", artifact=agent, name="other", subject=f"agent:{agent}", origin=Origin(f"agents/{agent}.yml"))


def _blocking(*diagnostics: Diagnostic) -> tuple[Diagnostic, ...]:
    return tuple(resolve_severities(DiagnosticBag(diagnostics), strict=True)[0])


def test_a_blocking_diagnostic_an_entry_for_another_artifact_of_its_type_matched_is_pointed_at_it() -> None:
    old, other = _toolset("concierge"), _toolset("greeter")
    entries = tuple(BaselineEntry(stable_fingerprint(item), item.code, item.subject or "") for item in (old, other))
    stray = BaselineEntry("0" * 16, "SST-VAL528", "")
    renamed = Baseline("b.json", "2030-01-01", (*entries, stray, BaselineEntry("1" * 16, "SST-CFG001", "agent:x")))
    found = match_baseline(
        _blocking(_toolset("host"), _toolset("host"), _toolset("host2")), renamed, today=TODAY, warn_from=WARN_FROM
    )
    # Once per code and artifact; an entry naming no artifact, or of an error, points nowhere.
    assert [(item.code, item.message) for item in found.hints] == [
        ("SST-INT011", "SST-VAL528 on agent:host may be what was baselined under agent:concierge, agent:greeter"),
        ("SST-INT011", "SST-VAL528 on agent:host2 may be what was baselined under agent:concierge, agent:greeter"),
    ]
    assert (found.notices, found.hints[0].subject) == ((), "config:b.json")


def test_no_hint_for_a_held_or_unblocking_diagnostic_or_an_entry_that_still_matches() -> None:
    old, new = _toolset("concierge"), _toolset("host")
    baseline = Baseline("b.json", "2030-01-01", (BaselineEntry(stable_fingerprint(old), old.code, "agent:concierge"),))

    def hints(*diagnostics: Diagnostic) -> list[str]:
        return [item.code for item in match_baseline(diagnostics, baseline, today=TODAY, warn_from=WARN_FROM).hints]

    assert hints(new) == []
    assert hints(*_blocking(old, new)) == []
    assert hints(*_blocking(D("SST-CFG003", key="k", subject="config:k"))) == []
    assert hints(*_blocking(D("SST-CFG003", key="k"))) == []
