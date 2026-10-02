"""SST-INT007: a diagnostic's severity was changed outside `resolve_severities`."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity, audit, resolve_severities


def test_sst_int007_fires() -> None:
    demoted = replace(D("SST-REF001", model="orders", subject="metric:m"), severity=Severity.WARNING)
    [_, found] = audit(DiagnosticBag((demoted,)))
    assert (found.code, found.severity, found.subject) == ("SST-INT007", Severity.ERROR, "metric:m")
    assert found.message == "SST-REF001 resolved severity outside diagnostics/"


def test_sst_int007_silent() -> None:
    promoted, count = resolve_severities(DiagnosticBag((D("SST-LOD003", file="a.yml"),)), strict=True)
    assert count == 1 and audit(promoted) is promoted
