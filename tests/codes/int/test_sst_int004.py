"""SST-INT004: a diagnostic was constructed without `D`."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity, audit


def test_sst_int004_fires() -> None:
    direct = Diagnostic("SST-REF001", Severity.ERROR, MappingProxyType({"model": "orders"}), subject="metric:m")
    [_, found] = audit(DiagnosticBag((direct,)))
    assert (found.code, found.severity, found.subject) == ("SST-INT004", Severity.ERROR, "metric:m")
    assert found.message == "diagnostic for SST-REF001 was constructed directly"


def test_sst_int004_silent() -> None:
    emitted = DiagnosticBag((D("SST-REF001", model="orders", subject="metric:m"),))
    assert audit(emitted) is emitted
