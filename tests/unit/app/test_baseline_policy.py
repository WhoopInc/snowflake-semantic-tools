"""The baseline gate and the project policy, decided in `app` with the clock injected."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.baseline import clock_stamp, clock_today, days_after, gate_baseline
from snowflake_semantic_tools.app.policy import hold_to_policy, severity_policy, strict_disagreement
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity, resolve_severities
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry, stable_fingerprint
from snowflake_semantic_tools.domain.ports.project import ProjectConfig

WARNING = D("SST-CFG018", group="g", name="m")
ERROR = D("SST-REF001", model="orders", subject="metric:m")


class _Clock:
    """A clock that reads one fixed instant."""

    def __init__(self, instant: str) -> None:
        self.instant = instant

    def now_iso(self) -> str:
        return self.instant

    def monotonic_ms(self) -> int:
        return 0

    def sleep(self, milliseconds: int) -> None:
        return None

    def new_run_id(self) -> str:
        return "run"


def _baseline(expires_on: str, *diagnostics: Diagnostic) -> Baseline:
    entries = tuple(BaselineEntry(stable_fingerprint(item), item.code) for item in diagnostics)
    return Baseline(".sst/baseline.json", expires_on, entries)


def _config(tree: dict[str, object]) -> ProjectConfig:
    return ProjectConfig(MappingProxyType(tree), DiagnosticBag(), True)


CLOCK = _Clock("2026-10-03T12:34:56.789012Z")


def test_the_clock_dates_the_baseline() -> None:
    today = clock_today(CLOCK)
    assert (today.isoformat(), days_after(today, 30), clock_stamp(CLOCK)) == (
        "2026-10-03",
        "2026-11-02",
        "2026-10-03T12:34:56Z",
    )


def test_a_baseline_holding_every_blocking_diagnostic_forgives_the_run() -> None:
    promoted, _ = resolve_severities(DiagnosticBag((WARNING,)), strict=True)
    gate = gate_baseline(promoted, _baseline("2999-01-01", WARNING), clock=CLOCK)
    assert (gate.forgiven, gate.expired, gate.diagnostics) == (True, False, promoted)
    assert gate.baselined == {stable_fingerprint(WARNING)}


def test_nothing_is_forgiven_without_errors_or_with_one_the_baseline_cannot_hold() -> None:
    assert gate_baseline(DiagnosticBag((WARNING,)), _baseline("2999-01-01", WARNING), clock=CLOCK).forgiven is False
    assert gate_baseline(DiagnosticBag((ERROR,)), _baseline("2999-01-01", ERROR), clock=CLOCK).forgiven is False


def test_an_expired_baseline_blocks_and_one_nearing_expiry_only_warns() -> None:
    expired = gate_baseline(DiagnosticBag((WARNING,)), _baseline("2026-10-02", WARNING), clock=CLOCK)
    assert (expired.expired, [item.code for item in expired.diagnostics]) == (True, ["SST-CFG039", "SST-CFG018"])
    nearing = gate_baseline(DiagnosticBag((WARNING,)), _baseline("2026-11-01", WARNING), clock=CLOCK)
    assert (nearing.expired, nearing.diagnostics[0].code) == (False, "SST-CFG035")


def test_the_policy_applies_overrides_and_says_whether_a_gated_result_blocks() -> None:
    config = _config({"diagnostics": {"severity_overrides": {"SST-REF001": "warning"}}})
    held = hold_to_policy(
        "validate", DiagnosticBag((ERROR,)), config, gated=True, promoted=0, baselined=False, notice_due=True
    )
    assert (held.blocks, held.notice_given, held.diagnostics[0].severity) == (False, False, Severity.WARNING)
    ungated = hold_to_policy(
        "compile", DiagnosticBag((ERROR,)), config, gated=False, promoted=0, baselined=False, notice_due=True
    )
    assert ungated.blocks is None
    plain = hold_to_policy(
        "validate", DiagnosticBag((ERROR,)), _config({}), gated=True, promoted=0, baselined=False, notice_due=True
    )
    assert (plain.blocks, plain.diagnostics) == (None, (ERROR,))
    assert severity_policy(config.tree, strict=True).overrides == {"SST-REF001": Severity.WARNING}


def test_the_strict_notice_is_given_once_to_a_strict_project_without_a_baseline() -> None:
    config = _config({"validation": {"strict": True}})
    given = hold_to_policy("plan", DiagnosticBag(), config, gated=False, promoted=2, baselined=False, notice_due=True)
    assert given.notice_given and given.diagnostics[0].message == (
        "validation.strict: true is enforced from 1.0; 2 warnings now block"
    )
    for command, baselined, due in (("compile", False, True), ("plan", True, True), ("plan", False, False)):
        quiet = hold_to_policy(
            command, DiagnosticBag(), config, gated=False, promoted=2, baselined=baselined, notice_due=due
        )
        assert not quiet.notice_given and quiet.diagnostics == ()


def test_a_strict_flag_that_contradicts_the_config_is_reported() -> None:
    config = _config({"validation": {"strict": True}})
    [found] = strict_disagreement(config, False)
    assert (found.code, found.subject) == ("SST-CFG034", "config:validation.strict")
    assert strict_disagreement(config, True) == () and strict_disagreement(config, None) == ()
    assert strict_disagreement(_config({}), False) == ()
