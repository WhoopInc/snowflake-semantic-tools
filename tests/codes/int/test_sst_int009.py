"""SST-INT009: a baseline entry matched more than one diagnostic."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity, apply_baseline

WARNINGS = DiagnosticBag((D("SST-LOD003", file="a.yml"), D("SST-LOD003", file="b.yml")))


def test_sst_int009_fires() -> None:
    # The same warning reported twice has one fingerprint, so one entry matches both.
    repeated = DiagnosticBag((D("SST-LOD003", file="a.yml"), D("SST-LOD003", file="a.yml")))
    entry = repeated[0].fingerprint[:16]
    result = apply_baseline(repeated, (entry,))
    found = result[-1]
    assert (found.code, found.severity) == ("SST-INT009", Severity.ERROR)
    assert found.message == f"baseline entry {entry} matched 2 diagnostics"
    assert not any(item.baselined for item in result)


def test_sst_int009_silent() -> None:
    entry = WARNINGS[0].fingerprint[:16]
    result = apply_baseline(WARNINGS, (entry,))
    assert [item.code for item in result] == ["SST-LOD003", "SST-LOD003"]
    assert [item.baselined for item in result] == [True, False]
