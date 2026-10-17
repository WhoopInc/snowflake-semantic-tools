"""Internal codes (INT): a defect in SST itself, never a problem with the project.

Every one is a non-demotable error. `D` emits SST-INT900 for an unregistered code and SST-INT901
for missing template context, `audit` the emission invariants, and the CLI SST-INT001 for an
exception nothing expected and SST-INT010 for a refusal that names no code. The module is `int_`
so it does not shadow the builtin.
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
        condition="an exception no handler expects reaches the command runner; the command exits 1",
    ),
    spec(
        "SST-INT002",
        Severity.ERROR,
        "Pure function attempted I/O",
        "{value} attempted I/O from a pure ring",
        _BUG,
        demotable=False,
        condition="a domain call made inside a pure phase opened a file, a socket, or a process",
    ),
    spec(
        "SST-INT003",
        Severity.ERROR,
        "Phase produced an output that violates its contract",
        "{value} returned {found}, expected {expected}",
        _BUG,
        demotable=False,
        condition=(
            "a compiler returns an artifact of a type the publish order does not place, or `sst format` "
            "would rewrite a file into a form that loads to a different value"
        ),
    ),
    spec(
        "SST-INT004",
        Severity.ERROR,
        "Diagnostic constructed outside the emit function",
        "diagnostic for {value} was constructed directly",
        _BUG,
        demotable=False,
        condition="a diagnostic a command reports was not built by `D`",
    ),
    spec(
        "SST-INT005",
        Severity.ERROR,
        "Diagnostic has no location where one is required",
        "{value} emitted with no location",
        _BUG,
        demotable=False,
        condition="a LOD, PRS, REF or VAL diagnostic names a file in its context and carries no origin",
    ),
    spec(
        "SST-INT007",
        Severity.ERROR,
        "Effective severity computed outside the diagnostics module",
        "{value} resolved severity outside diagnostics/",
        _BUG,
        demotable=False,
        condition=(
            "a diagnostic declares a severity other than its registered one, carries an override the "
            "demotion floor forbids, or has an effective severity that its override, strict promotion, "
            "or a cascade downgrade to info does not explain"
        ),
    ),
    spec(
        "SST-INT008",
        Severity.ERROR,
        "Cascade attribution produced a dangling cause chain",
        "{value} carries a caused_by naming no diagnostic in the run",
        _BUG,
        demotable=False,
        condition="a diagnostic carries a `caused_by` naming no diagnostic in the run",
    ),
    spec(
        "SST-INT009",
        Severity.ERROR,
        "Baseline entry matched more than one diagnostic",
        "baseline entry {value} matched {count} diagnostics",
        "re-generate the baseline",
        demotable=False,
        condition="a baseline entry's fingerprint matches more than one diagnostic in the run, so it baselines none",
    ),
    spec(
        "SST-INT010",
        Severity.ERROR,
        "Refusal reported with no code",
        "uncoded refusal: {detail}",
        _BUG,
        demotable=False,
        condition=(
            "a command refuses an unusable project, configuration, or saved plan with an error that "
            "carries no diagnostic of its own; the command still exits 4"
        ),
    ),
    spec(
        "SST-INT900",
        Severity.ERROR,
        "Unregistered code passed to the emit function",
        "unregistered code {value}",
        _BUG,
        demotable=False,
        condition="`D` is called with a code the registry does not contain",
    ),
    spec(
        "SST-INT901",
        Severity.ERROR,
        "Diagnostic context missing a required template placeholder",
        "{value} template needs {placeholder}, which was not supplied",
        _BUG,
        demotable=False,
        condition=(
            "`D` is called without a value for a placeholder the code's message template names, or "
            "a shared placeholder its suggestion names"
        ),
    ),
    spec(
        "SST-INT902",
        Severity.ERROR,
        "Domain invariant violated",
        "domain invariant violated: {detail}",
        _BUG,
        demotable=False,
        condition=(
            "rendering an artifact that validated raises an unexpected error, or apply meets a failure "
            "the plan should have recorded and did not"
        ),
    ),
)
