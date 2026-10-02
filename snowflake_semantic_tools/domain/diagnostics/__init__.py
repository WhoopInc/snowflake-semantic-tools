"""Structured diagnostics shared by every compiler phase.

A diagnostic is a registered code plus the context its message template formats. `core`
holds the value types and `spec`; `specs` holds every code, one module per code area;
`integrity` the checks the registry is built through. This module builds `ERROR_REGISTRY`
from those modules and re-exports the `core` names, so importers never reach into the
submodules. `D`, `Diagnostic`, and `render_diagnostic` look `ERROR_REGISTRY` up as a global
of this module on each call, so replacing it here changes what all three see.

`audit` and `unstable_fingerprints` check the emission invariants over a finished run: every
diagnostic came from `D`, carries the location it knows, has the severity the registry and
`resolve_severities` give it, and names a cause that is present.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from hashlib import sha256
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.diagnostics.core import (
    ERROR_REFERENCE_URL,
    ErrorSpec,
    Origin,
    RegistryIntegrityError,
    Severity,
    _placeholders,
)
from snowflake_semantic_tools.domain.diagnostics.integrity import check_error_specs
from snowflake_semantic_tools.domain.diagnostics.specs import (
    apl,
    cfg,
    dbt,
    dis,
    int_,
    lod,
    man,
    mem,
    pln,
    prs,
    prt,
    ref,
    reg,
    rnd,
    sno,
    val_agent,
    val_eval,
    val_filter,
    val_metric,
    val_relationship,
    val_semantic_view,
    val_shared,
    val_skill,
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
    "RULE_SETS",
    "RegistryIntegrityError",
    "Severity",
    "apply_baseline",
    "audit",
    "build_registry",
    "render_diagnostic",
    "resolve_severities",
    "unstable_fingerprints",
]

# Areas whose diagnostics point into a project file whenever they name one (SST-INT005).
_LOCATED_AREAS = frozenset({"LOD", "PRS", "REF", "VAL"})


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
        emitted: True when `D` built it, which `dataclasses.replace` keeps; `audit` reports one
            constructed any other way (SST-INT004). Not part of equality.
        baselined: True when the project's baseline accepts it, so strict mode leaves it alone.
            Not part of equality.
    """

    code: str
    severity: Severity
    context: Mapping[str, Any]
    origin: Origin | None = None
    subject: str | None = None
    related: tuple[Origin, ...] = ()
    caused_by: str | None = None
    emitted: bool = field(default=False, repr=False, compare=False)
    baselined: bool = field(default=False, repr=False, compare=False)

    @property
    def blocks(self) -> bool:
        """Report whether the diagnostic, at its resolved severity, blocks the command."""
        return self.severity is Severity.ERROR

    @property
    def cascaded(self) -> bool:
        """Report whether the diagnostic was downgraded to INFO as a consequence of another."""
        return self.caused_by is not None and self.severity is Severity.INFO

    @property
    def promoted_from(self) -> Severity | None:
        """Return the registered severity when the resolved one differs from it; None otherwise."""
        registered = ERROR_REGISTRY[self.code].severity
        return registered if self.severity is not registered else None

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

    def blocking_subjects(self) -> frozenset[str]:
        """Return the subjects that some blocking diagnostic names."""
        return frozenset(item.subject for item in self if item.blocks and item.subject is not None)


def build_registry(specs: tuple[ErrorSpec, ...]) -> Mapping[str, ErrorSpec]:
    """Index specs by code into a read-only registry, refusing one that contradicts itself.

    Raises:
        RegistryIntegrityError: Any of the faults `integrity.check_error_specs` names.
    """
    return check_error_specs(specs)


# The validation rule sets an artifact or member type's `validation_rules` names, each the VAL
# band whose codes check that kind of artifact or member (SST-REG006).
RULE_SETS: Mapping[str, tuple[ErrorSpec, ...]] = MappingProxyType(
    {
        "shared": val_shared.SPECS,
        "metric": val_metric.SPECS,
        "relationship": val_relationship.SPECS,
        "semantic_view": val_semantic_view.SPECS,
        "filter": val_filter.SPECS,
        "agent": val_agent.SPECS,
        "tool": val_tool.SPECS,
        "eval": val_eval.SPECS,
        "skill": val_skill.SPECS,
    }
)

# Iteration order is not a contract: the error reference sorts the codes of each section.
ERROR_REGISTRY = build_registry(
    (
        *reg.SPECS,
        *cfg.SPECS,
        *dis.SPECS,
        *lod.SPECS,
        *prs.SPECS,
        *ref.SPECS,
        *mem.SPECS,
        *dbt.SPECS,
        *val_shared.SPECS,
        *val_metric.SPECS,
        *val_relationship.SPECS,
        *val_semantic_view.SPECS,
        *val_filter.SPECS,
        *val_agent.SPECS,
        *val_tool.SPECS,
        *val_eval.SPECS,
        *val_skill.SPECS,
        *rnd.SPECS,
        *pln.SPECS,
        *apl.SPECS,
        *man.SPECS,
        *sno.SPECS,
        *prt.SPECS,
        *int_.SPECS,
    )
)


def D(
    code: str,
    /,
    *,
    origin: Origin | None = None,
    subject: str | None = None,
    related: tuple[Origin, ...] = (),
    caused_by: str | None = None,
    **context: Any,
) -> Diagnostic:
    """Construct one diagnostic from a registered code and template context.

    `code` is positional, so a template may name a `{code}` placeholder of its own.
    A LOD, PRS, REF or VAL code given no `origin` points at the `file`, `line` and `col` its
    context names, so a location the raise site knows is never dropped.
    """
    spec = ERROR_REGISTRY.get(code)
    if spec is None:
        return D("SST-INT900", value=code)
    missing = sorted(_placeholders(spec.template) - set(context))
    if missing:
        return D("SST-INT901", value=code, placeholder=", ".join(missing))
    if origin is None and spec.subsystem in _LOCATED_AREAS:
        origin = _context_origin(context)
    return Diagnostic(
        code=code,
        severity=spec.severity,
        context=MappingProxyType(dict(context)),
        origin=origin,
        subject=subject,
        related=related,
        caused_by=caused_by,
        emitted=True,
    )


def _context_origin(context: Mapping[str, Any]) -> Origin | None:
    file = context.get("file")
    if not isinstance(file, str) or not file:
        return None
    line = _position(context.get("line"))
    return Origin(file, line, _position(context.get("col")) if line is not None else None)


def _position(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def resolve_severities(diagnostics: DiagnosticBag, *, strict: bool) -> tuple[DiagnosticBag, int]:
    """Apply strict-mode promotion once, preserving non-demotable errors and baselined warnings."""
    if not strict:
        return diagnostics, 0
    promoted = tuple(
        replace(diagnostic, severity=Severity.ERROR)
        if diagnostic.severity is Severity.WARNING and not diagnostic.baselined
        else diagnostic
        for diagnostic in diagnostics
    )
    return DiagnosticBag(promoted), sum(
        before.severity is Severity.WARNING and after.severity is Severity.ERROR
        for before, after in zip(diagnostics, promoted, strict=True)
    )


def apply_baseline(diagnostics: DiagnosticBag, entries: Iterable[str]) -> DiagnosticBag:
    """Mark each warning or info a baseline entry matches as baselined, before strict mode runs.

    An entry is a fingerprint, or a prefix of one such as the 16 characters the JSON envelope
    prints. An error is never baselined. An entry that matches exactly one diagnostic marks it;
    one that matches several marks none of them.

    Diagnostics:
        SST-INT009: a baseline entry matched more than one diagnostic, one per entry in order.
    """
    marked: set[int] = set()
    found: list[Diagnostic] = []
    for entry in dict.fromkeys(entries):
        matches = [
            index
            for index, item in enumerate(diagnostics)
            if item.severity is not Severity.ERROR and item.fingerprint.startswith(entry)
        ]
        if len(matches) > 1:
            found.append(D("SST-INT009", value=entry, count=len(matches)))
        elif matches:
            marked.update(matches)
    if not marked and not found:
        return diagnostics
    kept = (replace(item, baselined=True) if index in marked else item for index, item in enumerate(diagnostics))
    return DiagnosticBag((*kept, *found))


def audit(diagnostics: DiagnosticBag) -> DiagnosticBag:
    """Return `diagnostics` followed by one internal error per emission invariant they break.

    Each diagnostic is checked in order; then the run's cause chains, once.

    Diagnostics:
        SST-INT004: a diagnostic was not built by `D`.
        SST-INT005: a LOD, PRS, REF or VAL diagnostic names a file in its context and has no origin.
        SST-INT007: a severity differs from the registered one other than by strict promotion
            of a warning or a cascade downgrade to INFO.
        SST-INT008: the first diagnostic whose `caused_by` names no code in the run.
    """
    found: list[Diagnostic] = []
    for item in diagnostics:
        spec = ERROR_REGISTRY[item.code]
        if not item.emitted:
            found.append(D("SST-INT004", value=item.code, subject=item.subject))
        if item.origin is None and spec.subsystem in _LOCATED_AREAS and isinstance(item.context.get("file"), str):
            found.append(D("SST-INT005", value=item.code, subject=item.subject))
        if not _legal_severity(item, spec.severity):
            found.append(D("SST-INT007", value=item.code, subject=item.subject))
    present = {item.code for item in diagnostics}
    dangling = next(
        (item for item in diagnostics if item.caused_by is not None and item.caused_by not in present), None
    )
    if dangling is not None:
        found.append(D("SST-INT008", value=dangling.code, origin=dangling.origin, subject=dangling.subject))
    return DiagnosticBag((*diagnostics, *found)) if found else diagnostics


def _legal_severity(item: Diagnostic, registered: Severity) -> bool:
    """Report whether a resolved severity is one `resolve_severities` or a cascade could give."""
    if item.severity is registered:
        return True
    if registered is Severity.WARNING and item.severity is Severity.ERROR:
        return True
    return item.cascaded


def unstable_fingerprints(first: Iterable[Diagnostic], second: Iterable[Diagnostic]) -> DiagnosticBag:
    """Return one SST-INT006 per code whose fingerprints differ between two identical runs.

    The runs are compared as multisets of fingerprints per code, so order does not matter, and
    codes are reported in sorted order.
    """
    before: dict[str, Counter[str]] = {}
    after: dict[str, Counter[str]] = {}
    for prints, items in ((before, first), (after, second)):
        for item in items:
            prints.setdefault(item.code, Counter())[item.fingerprint] += 1
    changed = sorted(code for code in set(before) | set(after) if before.get(code) != after.get(code))
    return DiagnosticBag(D("SST-INT006", value=code) for code in changed)


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
