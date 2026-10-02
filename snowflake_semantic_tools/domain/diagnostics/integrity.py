"""Integrity checks over the registries SST is built from, each raising one SST-REG code.

`registry_fault` builds the `RegistryIntegrityError` for a REG code, formatting the code's
template from `specs.reg` directly: these checks run while `ERROR_REGISTRY` is being built, so
`D` cannot be used. `check_error_specs` is what the package `__init__` builds `ERROR_REGISTRY`
through; `severity_comparisons` scans module source for SST-REG015, which needs the package's
files and so runs from the test suite rather than at import.

The checks run in a fixed order and the first failure raises, so a registry with several
faults always reports the same one.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, RegistryIntegrityError, Severity, _placeholders
from snowflake_semantic_tools.domain.diagnostics.specs import reg

CODE_SCHEME = re.compile(r"^SST-[A-Z]{3}\d{3}$")

# Numbers the 1.0 catalog retired, with the release that retired them. A retired number is
# burned: it keeps its meaning in user configuration, baselines and history, so it is never
# registered again (SST-REG017).
RETIRED_CODES: Mapping[str, str] = MappingProxyType(
    dict.fromkeys(
        (
            "SST-APL101",
            "SST-APL102",
            "SST-CFG021",
            "SST-CFG022",
            "SST-CFG024",
            "SST-CFG026",
            "SST-CFG027",
            "SST-CFG028",
            "SST-CFG030",
            "SST-DBT007",
            "SST-DBT008",
            "SST-MEM102",
            "SST-PRS108",
            "SST-PRT007",
            "SST-REF016",
            "SST-REF017",
            "SST-REF021",
            "SST-REF024",
            "SST-REF025",
            "SST-REF200",
            "SST-REF201",
            "SST-SNO021",
            "SST-VAL313",
            "SST-VAL501",
            "SST-VAL502",
            "SST-VAL503",
            "SST-VAL504",
            "SST-VAL505",
            "SST-VAL620",
            "SST-VAL736",
            "SST-VAL749",
            "SST-VAL750",
            "SST-VAL751",
            "SST-VAL752",
            "SST-VAL753",
            "SST-VAL754",
            "SST-VAL756",
            "SST-VAL757",
        ),
        "1.0.0",
    )
)

# Every placeholder a message template may name. The vocabulary is shared so a JSON consumer can
# act on a diagnostic's params without parsing prose; a template naming anything else is refused
# (SST-REG016) rather than shipped with a value no raise site supplies.
PLACEHOLDERS = frozenset(
    {
        *("a", "area", "artifact", "b", "block", "blocker", "cls", "code", "col", "column", "command"),
        *("components", "config", "count", "covered", "cycle", "date", "declared", "detail"),
        *("direction", "elapsed_ms", "expected", "field", "file", "flag", "found", "function", "group"),
        *("identifier", "index", "key", "kind", "line", "member", "member_type", "member_types"),
        *("metric", "model", "module", "name", "offset", "other", "outside", "path", "placeholder"),
        *("policy", "position", "problem", "profile", "reason", "ref_function", "relation"),
        *("relationship", "root", "root_key", "row_count", "rule_id", "selector", "severity"),
        *("shadowed", "size", "step", "subsystem", "suggestion", "target", "text", "type", "types"),
        *("used", "value", "var", "version", "view"),
    }
)

# Areas whose codes may carry raw text beyond their template (SST-REG019).
INTERNAL_DETAIL_AREAS = frozenset({"INT", "SNO"})
# Areas whose 900-band codes must also be non-demotable: their failure means the engine or its
# registry cannot be trusted, so no setting may lower one (SST-REG014).
NON_DEMOTABLE_INTERNAL_AREAS = frozenset({"INT", "REG"})

_TEMPLATES: Mapping[str, str] = MappingProxyType({entry.code: entry.template for entry in reg.SPECS})


def registry_fault(fault: str, /, **context: object) -> RegistryIntegrityError:
    """Build the error for the REG code `fault`, its message formatted from the code's template."""
    return RegistryIntegrityError(fault, _TEMPLATES[fault].format(**context), context)


def check_error_specs(specs: tuple[ErrorSpec, ...]) -> Mapping[str, ErrorSpec]:
    """Index specs by code into a read-only registry, refusing one that contradicts itself.

    Each spec is checked in order, and its checks run in the order the codes are listed.

    Raises:
        RegistryIntegrityError: SST-REG012, a code is not ``SST-<AREA><NNN>``; SST-REG013, its
            subsystem is not its area; SST-REG001, it has no title or template; SST-REG002, it
            repeats; SST-REG017, it is retired; SST-REG014, it is a 900-band code that is not an
            error, or not a non-demotable one in INT or REG; SST-REG016, its template names a
            placeholder outside `PLACEHOLDERS`; SST-REG018, it is deprecated with no successor;
            SST-REG019, it declares internal detail outside INT and SNO.
    """
    registry: dict[str, ErrorSpec] = {}
    for entry in specs:
        _check_identity(entry)
        if entry.code in registry:
            raise registry_fault("SST-REG002", type=entry.code)
        _check_policy(entry)
        registry[entry.code] = entry
    return MappingProxyType(registry)


def _check_identity(entry: ErrorSpec) -> None:
    """Refuse a malformed code, a subsystem other than its area, and a missing title or template."""
    if not CODE_SCHEME.match(entry.code):
        raise registry_fault("SST-REG012", code=entry.code)
    if entry.subsystem != entry.code[4:7]:
        raise registry_fault("SST-REG013", code=entry.code, subsystem=entry.subsystem)
    for field in ("title", "template"):
        if not getattr(entry, field).strip():
            raise registry_fault("SST-REG001", type=entry.code, field=field)


def _check_policy(entry: ErrorSpec) -> None:
    """Refuse a retired number, a misdeclared 900-band code, and a contradictory declaration."""
    if entry.code in RETIRED_CODES:
        raise registry_fault("SST-REG017", code=entry.code, version=RETIRED_CODES[entry.code])
    area = entry.subsystem
    if entry.code[7] == "9" and (
        entry.severity is not Severity.ERROR or (area in NON_DEMOTABLE_INTERNAL_AREAS and entry.demotable)
    ):
        severity = entry.severity.name if not entry.demotable else f"{entry.severity.name} (demotable)"
        raise registry_fault("SST-REG014", code=entry.code, severity=severity)
    unknown = sorted(_placeholders(entry.template) - PLACEHOLDERS)
    if unknown:
        raise registry_fault("SST-REG016", code=entry.code, placeholder=", ".join(unknown))
    if entry.deprecated_in is not None and entry.superseded_by is None:
        raise registry_fault("SST-REG018", code=entry.code, version=entry.deprecated_in)
    if entry.internal_detail and area not in INTERNAL_DETAIL_AREAS:
        raise registry_fault("SST-REG019", code=entry.code, area=area)


def severity_comparisons(module: str, source: str) -> tuple[RegistryIntegrityError, ...]:
    """Return one SST-REG015 per comparison in `source` that re-derives a diagnostic's severity.

    A comparison counts when an operand is a `.severity` attribute or a `Severity` member: the
    blocking decision belongs to the diagnostics module (`Diagnostic.blocks`,
    `DiagnosticBag.has_errors`), which is the one place severity is resolved. Modules under
    `domain/diagnostics/` are where that decision lives, so the caller does not scan them.

    Args:
        module: How the error names the module, such as its dotted path.
        source: The module's source text.
    """
    faults: list[RegistryIntegrityError] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Compare) and any(_names_severity(item) for item in (node.left, *node.comparators)):
            faults.append(registry_fault("SST-REG015", module=f"{module}:{node.lineno}"))
    return tuple(faults)


def _names_severity(node: ast.expr) -> bool:
    if not isinstance(node, ast.Attribute):
        return False
    return node.attr == "severity" or (isinstance(node.value, ast.Name) and node.value.id == "Severity")
