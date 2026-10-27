"""SST-INT008: a diagnostic's cause names no diagnostic in the run."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity, audit


def test_sst_int008_fires() -> None:
    orphan = D("SST-REF001", model="orders", subject="metric:m", caused_by="SST-LOD001")
    [_, found] = audit(DiagnosticBag((orphan,)))
    assert (found.code, found.severity, found.subject) == ("SST-INT008", Severity.ERROR, "metric:m")
    assert found.message == "SST-REF001 carries a caused_by naming no diagnostic in the run"


def test_sst_int008_silent() -> None:
    root = D("SST-LOD001", file="a.yml", line=1, col=1, detail="bad")
    bag = DiagnosticBag((root, D("SST-REF001", model="orders", subject="metric:m", caused_by="SST-LOD001")))
    assert audit(bag) is bag
