"""Structured diagnostics shared by every compiler phase.

A diagnostic is a registered code plus the context its message template formats. `core`
holds the value types and `spec`; `specs` holds every code, one module per code family.
This module builds `ERROR_REGISTRY` from those modules and re-exports the `core` names,
so importers never reach into the submodules. `D`, `Diagnostic`, and `render_diagnostic`
look `ERROR_REGISTRY` up as a global of this module on each call, so replacing it here
changes what all three see.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256
from types import MappingProxyType
from typing import Any, Mapping

from .core import ERROR_REFERENCE_URL, ErrorSpec, Origin, RegistryIntegrityError, Severity, _placeholders
from .specs import (
    apl,
    cfg,
    internal,
    loading,
    man,
    pln,
    prs,
    ref,
    snowflake,
    val_agent,
    val_eval,
    val_extension,
    val_semantic_view,
    val_tool,
)

__all__ = [
    "D",
    "Diagnostic",
    "DiagnosticBag",
    "ERROR_REFERENCE_URL",
    "ERROR_REGISTRY",
    "ErrorSpec",
    "Origin",
    "RegistryIntegrityError",
    "Severity",
    "build_registry",
    "render_diagnostic",
    "resolve_severities",
]


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """One reported problem; build it with `D`, which takes the severity from the code's entry.

    The message, phase, and help URL are read from `ERROR_REGISTRY` on access, never stored.

    Attributes:
        severity: The registered severity, or ERROR once strict mode promotes a warning.
        context: The template's placeholder values, read-only.
        subject: The artifact key of the artifact or member concerned; None when there is none.
        related: Further locations involved, such as the repeats of a duplicate declaration.
        caused_by: The code of the diagnostic this one cascades from; None when it stands alone.
    """

    code: str
    severity: Severity
    context: Mapping[str, Any]
    origin: Origin | None = None
    subject: str | None = None
    related: tuple[Origin, ...] = ()
    caused_by: str | None = None

    @property
    def message(self) -> str:
        """Format the code's template with this diagnostic's context."""
        return ERROR_REGISTRY[self.code].template.format(**self.context)

    @property
    def phase(self) -> str:
        """Name the phase that reports the code: its subsystem, in lowercase."""
        return ERROR_REGISTRY[self.code].phase

    @property
    def help_url(self) -> str:
        """Link to the code's entry in the generated error reference."""
        return ERROR_REGISTRY[self.code].help_url

    @property
    def fingerprint(self) -> str:
        """Hash the code, subject, location, and message into an identity that is stable across runs."""
        identity = (
            self.code,
            self.subject or "",
            self.origin.file if self.origin else "",
            str(self.origin.line if self.origin else ""),
            str(self.origin.col if self.origin else ""),
            self.message,
        )
        return sha256("\x1f".join(identity).encode("utf-8")).hexdigest()


class DiagnosticBag(tuple[Diagnostic, ...]):
    """Immutable diagnostics with the aggregate operations every use case needs."""

    def count(self, severity: Severity) -> int:
        """Count the diagnostics of one severity."""
        return sum(diagnostic.severity is severity for diagnostic in self)

    @property
    def has_errors(self) -> bool:
        """Report whether any diagnostic is an error."""
        return self.count(Severity.ERROR) > 0


def build_registry(specs: tuple[ErrorSpec, ...]) -> Mapping[str, ErrorSpec]:
    """Index specs by code into a read-only registry, refusing duplicate and malformed codes.

    Raises:
        RegistryIntegrityError: A code repeats, or is not ``SST-`` followed by its subsystem.
    """
    registry: dict[str, ErrorSpec] = {}
    for spec in specs:
        if spec.code in registry:
            raise RegistryIntegrityError(f"duplicate error code {spec.code}")
        if not spec.code.startswith("SST-") or spec.code.split("-")[1][:3] != spec.subsystem:
            raise RegistryIntegrityError(f"invalid error code {spec.code}")
        registry[spec.code] = spec
    return MappingProxyType(registry)


# Iteration order is not a contract: the error reference sorts the codes of each section.
ERROR_REGISTRY = build_registry(
    (
        *cfg.SPECS,
        *prs.SPECS,
        *loading.SPECS,
        *ref.SPECS,
        *val_semantic_view.SPECS,
        *val_agent.SPECS,
        *val_tool.SPECS,
        *val_eval.SPECS,
        *val_extension.SPECS,
        *man.SPECS,
        *pln.SPECS,
        *apl.SPECS,
        *snowflake.SPECS,
        *internal.SPECS,
    )
)


def D(
    code: str,
    *,
    origin: Origin | None = None,
    subject: str | None = None,
    related: tuple[Origin, ...] = (),
    caused_by: str | None = None,
    **context: Any,
) -> Diagnostic:
    """Construct one diagnostic from a registered code and template context."""
    spec = ERROR_REGISTRY.get(code)
    if spec is None:
        return D("SST-INT900", value=code)
    missing = sorted(_placeholders(spec.template) - set(context))
    if missing:
        return D("SST-INT901", value=code, placeholder=", ".join(missing))
    return Diagnostic(
        code=code,
        severity=spec.severity,
        context=MappingProxyType(dict(context)),
        origin=origin,
        subject=subject,
        related=related,
        caused_by=caused_by,
    )


def resolve_severities(diagnostics: DiagnosticBag, *, strict: bool) -> tuple[DiagnosticBag, int]:
    """Apply strict-mode promotion once, preserving non-demotable errors."""
    if not strict:
        return diagnostics, 0
    promoted = tuple(
        replace(diagnostic, severity=Severity.ERROR) if diagnostic.severity is Severity.WARNING else diagnostic
        for diagnostic in diagnostics
    )
    return DiagnosticBag(promoted), sum(
        before.severity is Severity.WARNING and after.severity is Severity.ERROR
        for before, after in zip(diagnostics, promoted)
    )


def render_diagnostic(diagnostic: Diagnostic) -> str:
    """Render one diagnostic as terminal text: a located headline, then its help and docs lines.

    The location is ``file:line:col: ``, cut to what the origin knows and absent without
    one; the help line is absent when the code has no suggestion.

    Example:
        views.yml:4:7: error[SST-REF001]: {{ ref('missing') }} is not a model in the dbt manifest
          help: <the code's suggestion>
          docs: <the code's help URL>
    """
    location = ""
    if diagnostic.origin is not None:
        location = diagnostic.origin.file
        if diagnostic.origin.line is not None:
            location += f":{diagnostic.origin.line}"
            if diagnostic.origin.col is not None:
                location += f":{diagnostic.origin.col}"
        location += ": "
    spec = ERROR_REGISTRY[diagnostic.code]
    rendered = f"{location}{diagnostic.severity.name.lower()}[{diagnostic.code}]: {diagnostic.message}"
    if spec.suggestion:
        rendered += f"\n  help: {spec.suggestion}"
    rendered += f"\n  docs: {diagnostic.help_url}"
    return rendered
