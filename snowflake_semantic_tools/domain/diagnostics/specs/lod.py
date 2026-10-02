"""Loading codes (LOD): file bytes, YAML syntax, document shape, template syntax, and sidecars."""

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
        "SST-LOD006",
        Severity.ERROR,
        "File is not valid UTF-8",
        "{file}: invalid UTF-8 at byte {offset}",
        "re-save the file as UTF-8",
    ),
    spec(
        "SST-LOD007",
        Severity.ERROR,
        "File exceeds the size limit",
        "{file} is {size} bytes, over the {expected} limit",
        "split the file",
    ),
    spec(
        "SST-LOD008",
        Severity.ERROR,
        "Multi-document YAML stream",
        "{file} contains {count} documents",
        "keep one document per file",
    ),
    spec(
        "SST-LOD009",
        Severity.ERROR,
        "Unquoted colon inside a plain scalar",
        "{file}:{line}: '{key}' value contains an unquoted ':'",
        "quote the value",
    ),
    spec(
        "SST-LOD010",
        Severity.ERROR,
        "Tab character used for indentation",
        "{file}:{line}: tab used for indentation",
        "indent with spaces",
    ),
    spec(
        "SST-LOD011",
        Severity.WARNING,
        "Folded scalar used for a multi-line value",
        "{file}:{line}: '{key}' uses a folded scalar",
        "use |- so line breaks survive",
    ),
    spec(
        "SST-LOD012",
        Severity.WARNING,
        "Trailing whitespace or CRLF line endings",
        "{file}: {detail}",
        "remove the trailing whitespace and save the file with LF line endings",
    ),
    spec(
        "SST-LOD013",
        Severity.ERROR,
        "Anchor or alias used",
        "{file}:{line}: YAML anchors are not supported",
        "expand the anchor: write the value out wherever the alias stands",
    ),
    spec(
        "SST-LOD014",
        Severity.ERROR,
        "Merge key used",
        "{file}:{line}: merge keys are not supported",
        "expand the merge: write the merged keys into the mapping",
    ),
    spec(
        "SST-LOD015",
        Severity.ERROR,
        "Non-string mapping key",
        "{file}:{line}: mapping key {found} is not a string",
        "quote the key",
    ),
    spec(
        "SST-LOD016",
        Severity.WARNING,
        "Implicit boolean or null coercion",
        "{file}:{line}: '{key}' value {found} coerced to {expected}",
        "quote the value if it is meant as a string",
    ),
    spec("SST-LOD017", Severity.ERROR, "Byte-order mark present", "{file} begins with a BOM", "re-save without a BOM"),
    spec(
        "SST-LOD018",
        Severity.ERROR,
        "Sidecar file referenced by a document is missing",
        "{file} references {path}, which does not exist",
        "create the file, or correct the path",
    ),
    spec(
        "SST-LOD019",
        Severity.ERROR,
        "Sidecar file is empty",
        "{path}, referenced by {file}, is empty",
        "add content, or remove the reference",
    ),
    spec(
        "SST-LOD020",
        Severity.WARNING,
        "Unrecognised file extension accepted",
        "{file} uses '{found}'",
        "standardise on .yml",
    ),
    spec(
        "SST-LOD021",
        Severity.ERROR,
        "Document declares no recognised root key",
        "{file} declares no recognised root key",
        "add the artifact's root key, or move the file out of the semantic models directory",
    ),
    spec("SST-LOD200", Severity.INFO, ".yaml extension accepted", "{file} uses .yaml; accepted", None),
    spec("SST-LOD201", Severity.INFO, "File loaded from cache", "{file} served from the load cache", None),
    spec(
        "SST-LOD202",
        Severity.INFO,
        "Line-ending normalisation applied on read",
        "{file} normalised {count} line endings on read",
        None,
    ),
)
