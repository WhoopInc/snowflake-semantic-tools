"""Loading codes (LOD): YAML syntax, document shape, template syntax, and sidecars."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Loading"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-LOD001",
        Severity.ERROR,
        "YAML syntax error",
        "{file}:{line}:{col}: {detail}",
        "fix the YAML syntax at the reported position",
    ),
    spec(
        "SST-LOD002",
        Severity.ERROR,
        "Document root is not a mapping",
        "{file} root is {found}, expected a mapping",
        "make the document a top-level mapping",
    ),
    spec("SST-LOD003", Severity.WARNING, "File is empty", "{file} is empty", "add content, or delete the file"),
    spec(
        "SST-LOD004",
        Severity.ERROR,
        "Template expression is malformed",
        "{file}:{line}:{col}: malformed template: {reason}",
        "close the template, remove nesting, or correct the call grammar",
    ),
    spec(
        "SST-LOD005",
        Severity.ERROR,
        "Duplicate key in a YAML mapping",
        "{file}:{line}: duplicate key '{key}'",
        "remove one of the two keys",
    ),
    spec(
        "SST-LOD008",
        Severity.ERROR,
        "Multi-document YAML stream",
        "{file} contains {count} documents",
        "keep one document per file",
    ),
    spec(
        "SST-LOD018",
        Severity.ERROR,
        "Referenced sidecar is missing",
        "{file} references {path}, which does not exist",
        "create the file, or correct the path",
    ),
    spec(
        "SST-LOD019",
        Severity.ERROR,
        "Referenced sidecar is empty",
        "{path}, referenced by {file}, is empty",
        "add content, or remove the reference",
    ),
)
