"""Apply a project's severity policy: its per-code overrides, then strict promotion.

Effective severity resolves in one order, in this module, before any command reads it:

    declared severity (registry)
      -> per-code override (`diagnostics.severity_overrides`)
      -> strict promotion (`--strict`, `validation.strict`)
      = effective severity

An applied override is recorded on the diagnostic, so `audit` can tell a legal override from a
severity changed anywhere else. An override the demotion floor forbids is never applied; the
configuration check reports it (SST-CFG033). Applying a policy twice changes nothing more.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import (
    Diagnostic,
    DiagnosticBag,
    Severity,
    override_refusal,
    resolve_severities,
)


@dataclass(frozen=True, slots=True)
class SeverityPolicy:
    """What a project changes about the severities its diagnostics report at.

    Attributes:
        overrides: The severity each overridden code reports at, by code.
        strict: Whether every warning, after its override, is promoted to an error.
    """

    overrides: Mapping[str, Severity] = field(default_factory=lambda: MappingProxyType({}))
    strict: bool = False


def apply_overrides(diagnostics: DiagnosticBag, overrides: Mapping[str, Severity]) -> DiagnosticBag:
    """Return `diagnostics` with each overridden code at the severity the project sets for it.

    A diagnostic already overridden, cascaded to INFO, or whose override the demotion floor
    forbids is left as it is. A warning strict mode already promoted is resolved again from its
    override, so an error demoted to a warning under strict still blocks.
    """
    if not overrides:
        return diagnostics
    return DiagnosticBag(_overridden(item, overrides.get(item.code)) for item in diagnostics)


def _overridden(item: Diagnostic, wanted: Severity | None) -> Diagnostic:
    if wanted is None or item.override is not None or item.cascaded:
        return item
    if override_refusal(item.code, wanted) is not None:
        return item
    promoted = item.severity is Severity.ERROR and item.promoted_from is Severity.WARNING
    effective = Severity.ERROR if promoted and wanted is Severity.WARNING else wanted
    return replace(item, severity=effective, override=wanted)


def apply_policy(diagnostics: DiagnosticBag, policy: SeverityPolicy) -> tuple[DiagnosticBag, int]:
    """Resolve every diagnostic's effective severity: its override, then strict promotion.

    Returns:
        The diagnostics, and how many warnings strict mode made errors.
    """
    return resolve_severities(apply_overrides(diagnostics, policy.overrides), strict=policy.strict)
