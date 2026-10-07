"""Writing a baseline: additive `add`, `prune` of what no longer matches, `renew`, and the refusals."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    Baseline,
    BaselineEntry,
    Renewal,
    baseline_entry,
    baseline_refusal,
    chosen_for_baseline,
    renewed,
    stable_fingerprint,
    with_entries,
    without_stale,
)

EMPTY = Baseline(".sst/baseline.json", "2027-01-01", ())


def _warning(key: str) -> Diagnostic:
    return D("SST-CFG003", key=key, origin=Origin("sst_config.yml", 4), subject=f"config:{key}")


def test_an_entry_records_what_a_reviewer_reads_it_by() -> None:
    diagnostic = _warning("a")
    entry = baseline_entry(diagnostic, "known")
    assert entry == BaselineEntry(stable_fingerprint(diagnostic), "SST-CFG003", "config:a", "sst_config.yml", "known")
    bare = D("SST-CFG037", count=1)
    assert baseline_entry(bare, "") == BaselineEntry(stable_fingerprint(bare), "SST-CFG037", "", "", "")


def test_add_is_additive_skips_errors_ambiguous_and_recorded_fingerprints() -> None:
    first, second = _warning("a"), _warning("b")
    error = D("SST-CFG001", subject="config:discovery", path="x")
    start, added = with_entries(EMPTY, [first], lambda code: f"note {code}")
    assert [entry.note for entry in added] == ["note SST-CFG003"]
    twice = [second, second, first, error]
    grown, more = with_entries(start, twice, lambda code: "later")
    assert more == ()
    assert grown.entries == start.entries


def test_prune_removes_only_stale_entries_in_scope() -> None:
    kept, stale, outside = _warning("kept"), _warning("stale"), _warning("outside")
    baseline, _ = with_entries(EMPTY, [kept, stale, outside], lambda code: "")
    pruned, removed = without_stale(baseline, [kept], lambda entry: entry.artifact != "config:outside")
    assert [entry.artifact for entry in removed] == ["config:stale"]
    assert [entry.artifact for entry in pruned.entries] == ["config:kept", "config:outside"]


def test_renew_redates_and_records_the_reason() -> None:
    once = renewed(EMPTY, today="2026-10-02", expires_on="2027-04-01", reason="tracked")
    twice = renewed(once, today="2026-11-01", expires_on="2027-05-01", reason="again")
    assert twice.expires_on == "2027-05-01"
    assert twice.renewals == (
        Renewal("2026-10-02", "tracked", "2027-04-01"),
        Renewal("2026-11-01", "again", "2027-05-01"),
    )


def test_errors_and_non_demotable_codes_cannot_be_baselined() -> None:
    assert baseline_refusal("SST-CFG003") is None
    assert baseline_refusal("SST-VAL116") == (
        "SST-VAL116 is an error; a baseline suppresses and never demotes, so use severity_overrides"
    )


def test_a_non_demotable_warning_cannot_be_baselined(monkeypatch: pytest.MonkeyPatch) -> None:
    import dataclasses

    from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY

    spec = dataclasses.replace(ERROR_REGISTRY["SST-CFG003"], demotable=False)
    registry = {**ERROR_REGISTRY, "SST-CFG003": spec}
    monkeypatch.setattr("snowflake_semantic_tools.domain.diagnostics.baseline.ERROR_REGISTRY", registry)
    assert baseline_refusal("SST-CFG003") == "SST-CFG003 is non-demotable, so no baseline may hold it"


def test_add_chooses_one_code_or_every_warning() -> None:
    warning, info = _warning("a"), D("SST-DIS200", path="x.yml", type="metric")
    assert chosen_for_baseline([warning, info], None) == (warning,)
    assert chosen_for_baseline([warning, info], "SST-DIS200") == (info,)


def test_an_entry_a_connected_run_added_is_judged_only_by_a_run_against_its_target() -> None:
    _, [connected] = with_entries(EMPTY, [_warning("a")], lambda code: "", "verify")
    offline = baseline_entry(_warning("b"), "")
    assert (connected.connected, connected.target, offline.connected) == (True, "verify", False)
    assert connected.judged_by("verify") and not connected.judged_by(None) and not connected.judged_by("prod")
    assert offline.judged_by(None) and not offline.judged_by("verify")
