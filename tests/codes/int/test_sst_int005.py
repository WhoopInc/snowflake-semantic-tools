"""SST-INT005: a located diagnostic names a file in its context and points nowhere."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin, Severity, audit


def test_sst_int005_fires() -> None:
    unlocated = replace(D("SST-LOD003", file="views.yml"), origin=None)
    [_, found] = audit(DiagnosticBag((unlocated,)))
    assert (found.code, found.severity) == ("SST-INT005", Severity.ERROR)
    assert found.message == "SST-LOD003 emitted with no location"


def test_sst_int005_silent() -> None:
    # D points a located code at the file its context names; an unlocated area needs none.
    located = D("SST-LOD003", file="views.yml")
    assert located.origin == Origin("views.yml")
    bag = DiagnosticBag((located, D("SST-INT902", detail="x")))
    assert audit(bag) is bag
