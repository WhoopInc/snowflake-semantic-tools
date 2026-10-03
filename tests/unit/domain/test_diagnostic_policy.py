"""Severity policy: overrides resolve before strict promotion, are recorded, and the audit honours them."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity, audit, resolve_severities
from snowflake_semantic_tools.domain.diagnostics.policy import SeverityPolicy, apply_overrides, apply_policy

WARNING = D("SST-LOD003", file="a.yml")
ERROR = D("SST-REF001", model="orders", subject="metric:m")
INFO = D("SST-VAL020", rule_id="SST-VAL418", detail="off")
LOCKED = D("SST-VAL102", function="RANK", metric="m")


def _codes(bag: DiagnosticBag) -> list[str]:
    return [item.code for item in bag]


def test_d_records_the_declared_severity_and_no_override() -> None:
    assert (ERROR.declared, ERROR.override, ERROR.promoted_from, ERROR.demoted_from) == (
        Severity.ERROR,
        None,
        None,
        None,
    )


def test_an_override_sets_the_effective_severity_and_records_itself() -> None:
    overrides = MappingProxyType({"SST-REF001": Severity.WARNING, "SST-LOD003": Severity.INFO})
    demoted, quiet = apply_overrides(DiagnosticBag((ERROR, WARNING)), overrides)
    assert (demoted.severity, demoted.declared, demoted.override, demoted.demoted_from) == (
        Severity.WARNING,
        Severity.ERROR,
        Severity.WARNING,
        Severity.ERROR,
    )
    assert (quiet.severity, quiet.demoted_from, quiet.promoted_from) == (Severity.INFO, Severity.WARNING, None)
    promoted = apply_overrides(DiagnosticBag((INFO,)), {"SST-VAL020": Severity.WARNING})[0]
    assert (promoted.severity, promoted.promoted_from) == (Severity.WARNING, Severity.INFO)
    assert audit(DiagnosticBag((demoted, quiet, promoted))) == (demoted, quiet, promoted)


def test_applying_overrides_again_or_with_none_changes_nothing() -> None:
    bag = DiagnosticBag((ERROR,))
    assert apply_overrides(bag, {}) is bag
    once = apply_overrides(bag, {"SST-REF001": Severity.WARNING})
    assert apply_overrides(once, {"SST-REF001": Severity.ERROR}) == once


def test_a_forbidden_or_cascaded_override_is_never_applied() -> None:
    cascaded = replace(ERROR, severity=Severity.INFO, caused_by="SST-LOD003")
    overrides = {"SST-VAL102": Severity.WARNING, "SST-REF001": Severity.WARNING}
    assert apply_overrides(DiagnosticBag((LOCKED, cascaded)), overrides) == (LOCKED, cascaded)
    assert apply_overrides(DiagnosticBag((ERROR,)), {"SST-REF001": Severity.INFO}) == (ERROR,)


def test_strict_applies_after_the_override() -> None:
    policy = SeverityPolicy(MappingProxyType({"SST-REF001": Severity.WARNING, "SST-LOD003": Severity.INFO}), True)
    (error, info), promoted = apply_policy(DiagnosticBag((ERROR, WARNING)), policy)
    assert (error.severity, error.override, info.severity, promoted) == (
        Severity.ERROR,
        Severity.WARNING,
        Severity.INFO,
        1,
    )
    assert audit(DiagnosticBag((error, info))) == (error, info)
    assert apply_policy(DiagnosticBag((ERROR,)), SeverityPolicy()) == (DiagnosticBag((ERROR,)), 0)


def test_an_override_reaching_a_warning_strict_already_promoted_resolves_it_again() -> None:
    promoted, _ = resolve_severities(DiagnosticBag((WARNING,)), strict=True)
    kept = apply_overrides(promoted, {"SST-LOD003": Severity.WARNING})[0]
    quiet = apply_overrides(promoted, {"SST-LOD003": Severity.INFO})[0]
    raised = apply_overrides(promoted, {"SST-LOD003": Severity.ERROR})[0]
    assert (kept.severity, quiet.severity, raised.severity) == (Severity.ERROR, Severity.INFO, Severity.ERROR)
    assert audit(DiagnosticBag((kept, quiet, raised))) == (kept, quiet, raised)


def test_the_audit_refuses_a_false_declaration_or_a_forbidden_override() -> None:
    false_declaration = replace(WARNING, declared=Severity.ERROR)
    forbidden = replace(LOCKED, severity=Severity.WARNING, override=Severity.WARNING)
    unrecorded = replace(ERROR, severity=Severity.WARNING)
    off_policy = replace(ERROR, severity=Severity.INFO, override=Severity.WARNING)
    found = audit(DiagnosticBag((false_declaration, forbidden, unrecorded, off_policy)))
    assert _codes(found)[4:] == ["SST-INT007"] * 4


def test_a_diagnostic_built_without_d_reads_its_declared_severity_off_the_registry() -> None:
    built = replace(ERROR, declared=None, severity=Severity.WARNING)
    assert (built.demoted_from, built.promoted_from) == (Severity.ERROR, None)
