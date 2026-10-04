"""The emission audits and the derived properties diagnostics carry."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import (
    D,
    Diagnostic,
    DiagnosticBag,
    Origin,
    Severity,
    audit,
    resolve_severities,
    unstable_fingerprints,
)
from tests.helpers.diagnostic_filters import codes

WARNING = D("SST-LOD003", file="a.yml")
ERROR = D("SST-REF001", model="orders", subject="metric:m")


def test_d_points_a_located_code_at_the_file_line_and_column_its_context_names() -> None:
    assert D("SST-LOD001", file="a.yml", line=2, col=3, detail="x").origin == Origin("a.yml", 2, 3)
    assert D("SST-LOD001", file="a.yml", line=True, col=3, detail="x").origin == Origin("a.yml")
    assert D("SST-LOD004", file="", line=1, col=1, reason="x").origin is None
    assert D("SST-LOD003", file="a.yml", origin=Origin("b.yml")).origin == Origin("b.yml")
    assert D("SST-INT902", detail="x").origin is None


def test_blocking_cascade_and_promotion_are_read_off_the_resolved_severity() -> None:
    cascaded = replace(ERROR, severity=Severity.INFO, caused_by="SST-LOD001")
    promoted, _ = resolve_severities(DiagnosticBag((WARNING,)), strict=True)
    assert (ERROR.blocks, WARNING.blocks, cascaded.cascaded, ERROR.cascaded) == (True, False, True, False)
    assert (promoted[0].promoted_from, ERROR.promoted_from) == (Severity.WARNING, None)
    assert DiagnosticBag((ERROR, WARNING, D("SST-INT902", detail="x"))).blocking_subjects() == frozenset({"metric:m"})


def test_audit_reports_each_broken_invariant_once_and_leaves_a_clean_run_alone() -> None:
    clean = DiagnosticBag((WARNING, ERROR, replace(ERROR, severity=Severity.INFO, caused_by="SST-LOD003")))
    assert audit(clean) is clean
    direct = Diagnostic("SST-INT902", Severity.ERROR, MappingProxyType({"detail": "x"}))
    unlocated = replace(WARNING, origin=None)
    raised = replace(WARNING, severity=Severity.INFO)
    dangling = replace(ERROR, caused_by="SST-CFG003")
    found = audit(DiagnosticBag((direct, unlocated, raised, dangling, dangling)))
    assert codes(found)[5:] == ["SST-INT004", "SST-INT005", "SST-INT007", "SST-INT008"]


def test_fingerprints_are_compared_per_code_as_multisets() -> None:
    other = D("SST-LOD003", file="b.yml")
    assert unstable_fingerprints((WARNING, other), (other, WARNING)) == ()
    found = unstable_fingerprints((WARNING, ERROR), (other,))
    assert [item.context["value"] for item in found] == ["SST-LOD003", "SST-REF001"]


def test_audit_accepts_a_warning_strict_mode_promoted() -> None:
    promoted, _ = resolve_severities(DiagnosticBag((WARNING,)), strict=True)
    assert audit(promoted) is promoted
