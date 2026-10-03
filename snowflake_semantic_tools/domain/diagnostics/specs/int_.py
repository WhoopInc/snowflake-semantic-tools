"""Internal codes (INT): a defect in SST itself, never a problem with the project.

Every one is a non-demotable error. `D` emits SST-INT900 for an unregistered code and SST-INT901
for missing template context, `audit` the emission invariants, and the CLI SST-INT001 for an
exception nothing expected. The module is `int_` so it does not shadow the builtin.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Internal"

_BUG = "report this as a bug"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-INT001",
        Severity.ERROR,
        "Unhandled internal exception",
        "internal error: {detail}",
        "report this with the code and internal_detail",
        demotable=False,
        internal_detail=True,
    ),
    spec(
        "SST-INT002",
        Severity.ERROR,
        "Pure function attempted I/O",
        "{value} attempted I/O from a pure ring",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT003",
        Severity.ERROR,
        "Phase produced an output that violates its contract",
        "{value} returned {found}, expected {expected}",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT004",
        Severity.ERROR,
        "Diagnostic constructed outside the emit function",
        "diagnostic for {value} was constructed directly",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT005",
        Severity.ERROR,
        "Diagnostic has no location where one is required",
        "{value} emitted with no location",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT006",
        Severity.ERROR,
        "Fingerprint is not stable across identical runs",
        "fingerprint for {value} changed between identical runs",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT007",
        Severity.ERROR,
        "Effective severity computed outside the diagnostics module",
        "{value} resolved severity outside diagnostics/",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT008",
        Severity.ERROR,
        "Cascade attribution produced a dangling cause chain",
        "{value} carries a caused_by naming no diagnostic in the run",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT009",
        Severity.ERROR,
        "Baseline entry matched more than one diagnostic",
        "baseline entry {value} matched {count} diagnostics",
        "re-generate the baseline",
        demotable=False,
    ),
    spec(
        "SST-INT900",
        Severity.ERROR,
        "Unregistered code passed to the emit function",
        "unregistered code {value}",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT901",
        Severity.ERROR,
        "Diagnostic context missing a required template placeholder",
        "{value} template needs {placeholder}, which was not supplied",
        _BUG,
        demotable=False,
    ),
    spec(
        "SST-INT902",
        Severity.ERROR,
        "Domain invariant violated",
        "domain invariant violated: {detail}",
        _BUG,
        demotable=False,
    ),
)
