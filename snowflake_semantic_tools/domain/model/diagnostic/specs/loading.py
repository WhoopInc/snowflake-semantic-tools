"""Loading codes (LOD), dbt metadata codes (DBT), and membership codes (MEM).

LOD covers reading project files: YAML syntax, document shape, template syntax, and
sidecars. DBT covers the SST metadata on dbt models and columns. MEM covers membership:
a declared table that names no dbt model, or a member attached to no artifact. Each
prefix keeps its own section title in `SUBSYSTEMS`; `TITLE` is the title of LOD.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

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
        "SST-MEM003",
        Severity.ERROR,
        "Declared table does not name a known dbt model",
        "{member} declares table '{name}', which is not a known dbt model",
        "use a dbt model name the manifest knows",
    ),
    spec(
        "SST-MEM005",
        Severity.WARNING,
        "Member attached to zero artifacts",
        "{member} attaches to no {type}",
        "add its tables to a view, or delete the member",
    ),
    spec(
        "SST-DBT005",
        Severity.ERROR,
        "Key metadata is written in the 0.3 form",
        "model '{model}': meta.sst.{field} is written in the 0.3 form",
        "write primary_key as a list of columns and unique_keys as a list of column lists",
    ),
    spec(
        "SST-DBT030",
        Severity.ERROR,
        "Forbidden meta.sst location key",
        "model '{model}': meta.sst.{key} is forbidden -- delete it",
        "delete the key; relation location comes from dbt's resolved manifest",
    ),
    spec(
        "SST-DBT003",
        Severity.ERROR,
        "Unknown dbt semantic role",
        "model '{model}': meta.sst role '{found}' is not a known role",
        "correct the role name",
    ),
    spec(
        "SST-DBT004",
        Severity.WARNING,
        "dbt and semantic column types disagree",
        "model '{model}': column '{column}' is {found} in dbt and {expected} in the semantic layer",
        "reconcile the two types, or add a dbt contract",
    ),
    spec(
        "SST-DBT031",
        Severity.WARNING,
        "Model has no relation to enrich",
        "model '{model}' has no relation, so sst enrich has no columns to read",
        "materialize the model as a table or a view; an ephemeral model has nothing to enrich",
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
