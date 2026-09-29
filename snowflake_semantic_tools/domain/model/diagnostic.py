"""Structured diagnostics shared by every compiler phase."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import IntEnum
from hashlib import sha256
from string import Formatter
from types import MappingProxyType
from typing import Any, Mapping


class Severity(IntEnum):
    INFO = 10
    WARNING = 20
    ERROR = 30


@dataclass(frozen=True, slots=True)
class Origin:
    file: str
    line: int | None = None
    col: int | None = None
    dbt_node: str | None = None


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    code: str
    severity: Severity
    title: str
    template: str
    suggestion: str | None
    subsystem: str
    phase: str
    help_url: str
    demotable: bool = True


@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: str
    severity: Severity
    context: Mapping[str, Any]
    origin: Origin | None = None
    subject: str | None = None
    related: tuple[Origin, ...] = ()
    caused_by: str | None = None

    @property
    def message(self) -> str:
        return ERROR_REGISTRY[self.code].template.format(**self.context)

    @property
    def phase(self) -> str:
        return ERROR_REGISTRY[self.code].phase

    @property
    def help_url(self) -> str:
        return ERROR_REGISTRY[self.code].help_url

    @property
    def fingerprint(self) -> str:
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
        return sum(diagnostic.severity is severity for diagnostic in self)

    @property
    def has_errors(self) -> bool:
        return self.count(Severity.ERROR) > 0


class RegistryIntegrityError(RuntimeError):
    """The diagnostic registry itself is invalid."""


# Every code has a heading in the generated error reference (`sst docs`); the
# fragment is the code in lowercase, which is the anchor GitHub derives from it.
ERROR_REFERENCE_URL = "https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/reference/error-codes.md"


def _spec(
    code: str,
    severity: Severity,
    title: str,
    template: str,
    suggestion: str | None,
    *,
    demotable: bool = True,
) -> ErrorSpec:
    return ErrorSpec(
        code=code,
        severity=severity,
        title=title,
        template=template,
        suggestion=suggestion,
        subsystem=code.split("-")[1][:3],
        phase=code.split("-")[1][:3].casefold(),
        help_url=f"{ERROR_REFERENCE_URL}#{code.casefold()}",
        demotable=demotable,
    )


_SPECS = (
    _spec(
        "SST-CFG010",
        Severity.ERROR,
        "Profile or target not found",
        "target '{target}' is absent from profile '{profile}'",
        "add the target, or pass --target with a declared name",
    ),
    _spec(
        "SST-LOD001",
        Severity.ERROR,
        "YAML syntax error",
        "{file}:{line}:{col}: {detail}",
        "fix the YAML syntax at the reported position",
    ),
    _spec(
        "SST-LOD002",
        Severity.ERROR,
        "Document root is not a mapping",
        "{file} root is {found}, expected a mapping",
        "make the document a top-level mapping",
    ),
    _spec("SST-LOD003", Severity.WARNING, "File is empty", "{file} is empty", "add content, or delete the file"),
    _spec(
        "SST-LOD004",
        Severity.ERROR,
        "Template expression is malformed",
        "{file}:{line}:{col}: malformed template: {reason}",
        "close the template, remove nesting, or correct the call grammar",
    ),
    _spec(
        "SST-LOD005",
        Severity.ERROR,
        "Duplicate key in a YAML mapping",
        "{file}:{line}: duplicate key '{key}'",
        "remove one of the two keys",
    ),
    _spec(
        "SST-LOD008",
        Severity.ERROR,
        "Multi-document YAML stream",
        "{file} contains {count} documents",
        "keep one document per file",
    ),
    _spec(
        "SST-PRT007",
        Severity.ERROR,
        "dbt manifest schema version unsupported",
        "manifest schema '{found}' is unsupported; expected '{expected}'",
        "use a dbt version that emits {expected}",
    ),
    _spec(
        "SST-REF001",
        Severity.ERROR,
        "ref() model not in the dbt catalog",
        "{{{{ ref('{model}') }}}} is not a model in the dbt manifest",
        "run dbt parse, or correct the model name",
    ),
    _spec(
        "SST-REF005",
        Severity.ERROR,
        "metric() reference cycle",
        "metric reference cycle: {cycle}",
        "break the cycle",
    ),
    _spec(
        "SST-REF002",
        Severity.ERROR,
        "ref() column not on the model",
        "{{{{ ref('{model}','{column}') }}}}: '{column}' is not a column on {model}",
        "correct the column name, or add it to the model",
    ),
    _spec(
        "SST-MEM003",
        Severity.ERROR,
        "Declared table does not name a known dbt model",
        "{member} declares table '{name}', which is not a known dbt model",
        "use a dbt model name the manifest knows",
    ),
    _spec(
        "SST-PRS102",
        Severity.ERROR,
        "tables is explicitly empty",
        "{artifact}: 'tables: []' is not accepted",
        "declare the tables the member attaches to",
        demotable=False,
    ),
    _spec(
        "SST-PRS002",
        Severity.ERROR,
        "Required field missing",
        "{artifact}: required field '{field}' is missing",
        "add the required field",
    ),
    _spec(
        "SST-PRS003",
        Severity.ERROR,
        "Field has the wrong type",
        "{artifact}: '{field}' expects {expected}, found {found}",
        "change the value to the expected type",
    ),
    _spec(
        "SST-PRS013",
        Severity.ERROR,
        "Value outside allowed set",
        "{artifact}: '{field}' is '{found}', expected one of {expected}",
        "use an allowed value",
    ),
    _spec(
        "SST-PRS020",
        Severity.WARNING,
        "Deprecated field spelling",
        "{artifact}: '{field}' is deprecated; use '{expected}'",
        "rename the field",
    ),
    _spec(
        "SST-PRS029",
        Severity.ERROR,
        "Synonyms block has the wrong type",
        "{artifact}: synonyms must be a list of strings, found {found}",
        "use a list of strings",
    ),
    _spec(
        "SST-PRS030",
        Severity.WARNING,
        "Synonym contains problematic characters",
        "{artifact}: synonym '{value}' contains {detail}",
        "remove the punctuation",
    ),
    _spec(
        "SST-PRS110",
        Severity.ERROR,
        "Relationship condition shape is invalid",
        "{artifact}: condition '{value}' does not parse to one left/right pair",
        "express one pair per condition",
    ),
    _spec(
        "SST-PRS113",
        Severity.ERROR,
        "Expression field is not a string",
        "{artifact}: '{field}' expects a SQL string, found {found}",
        "supply the expression as a string",
    ),
    _spec(
        "SST-VAL001",
        Severity.ERROR,
        "Name is not unique within its type",
        "{type} '{name}' is declared more than once",
        "rename one of them",
    ),
    _spec(
        "SST-VAL003",
        Severity.WARNING,
        "Description missing",
        "{type} '{name}' has no description",
        "add a description; it is how Analyst chooses between objects",
    ),
    _spec(
        "SST-VAL101",
        Severity.ERROR,
        "Metric expression is not an aggregate",
        "metric '{metric}' is table-scoped and its expr is not an aggregate",
        "wrap the expression in an aggregate; compute window expressions in the dbt model",
    ),
    _spec(
        "SST-VAL102",
        Severity.ERROR,
        "Window function in a derived metric",
        "window function {function} in derived metric '{metric}'",
        "compute the window in the dbt model",
        demotable=False,
    ),
    _spec(
        "SST-VAL103",
        Severity.ERROR,
        "Derived metric aggregates another metric",
        "derived metric '{metric}' aggregates '{other}'",
        "reference the metric without an outer aggregate",
        demotable=False,
    ),
    _spec(
        "SST-VAL104",
        Severity.ERROR,
        "Derived metric references a physical column",
        "derived metric '{metric}' references column '{column}'",
        "reference metrics only, or make the metric table-scoped",
        demotable=False,
    ),
    _spec(
        "SST-VAL105",
        Severity.ERROR,
        "Derived metric references an un-aggregated member",
        "derived metric '{metric}' references un-aggregated {member_type} '{other}'",
        "aggregate it in a table-scoped metric first",
        demotable=False,
    ),
    _spec(
        "SST-VAL106",
        Severity.ERROR,
        "Regular metric references a derived metric",
        "metric '{metric}' is table-scoped and references derived metric '{other}'",
        "make the metric derived, or inline the expression",
        demotable=False,
    ),
    _spec(
        "SST-VAL107",
        Severity.ERROR,
        "Regular metric references a non-additive metric",
        "metric '{metric}' references '{other}', which declares non_additive_dimensions",
        "remove the reference, or drop the non-additive declaration",
        demotable=False,
    ),
    _spec(
        "SST-VAL108",
        Severity.ERROR,
        "Derived metric declares tables",
        "derived metric '{metric}' declares tables:",
        "remove tables; a derived metric is view-scoped",
    ),
    _spec(
        "SST-VAL109",
        Severity.ERROR,
        "Table-scoped metric declares no tables",
        "metric '{metric}' is table-scoped and declares no tables:",
        "declare tables explicitly",
    ),
    _spec(
        "SST-VAL110",
        Severity.WARNING,
        "Metric contains a bare identifier",
        "metric '{metric}' expression contains bare identifier '{column}'",
        "wrap it in a two-argument ref so it is checked",
    ),
    _spec(
        "SST-VAL112",
        Severity.ERROR,
        "Metric reaches an undeclared table",
        "metric '{metric}' in {artifact} reaches {outside}",
        "add the table to the view or narrow the expression",
    ),
    _spec(
        "SST-VAL113",
        Severity.ERROR,
        "Derived metric declares using_relationships",
        "derived metric '{metric}' declares using_relationships",
        "remove using_relationships; derived metrics have no join path",
    ),
    _spec(
        "SST-VAL114",
        Severity.ERROR,
        "Relationship path starts elsewhere",
        "metric '{metric}': relationship '{other}' does not start from '{name}'",
        "name a relationship whose left side is the metric table",
    ),
    _spec(
        "SST-VAL115",
        Severity.ERROR,
        "Relationship path is a chain",
        "metric '{metric}' declares a chain of {count} relationships",
        "declare one relationship, not a path",
    ),
    _spec(
        "SST-VAL121",
        Severity.ERROR,
        "Access modifier is invalid",
        "metric '{metric}': access_modifier is '{found}'",
        "use public_access or private_access",
    ),
    _spec(
        "SST-VAL122",
        Severity.WARNING,
        "Visibility key is deprecated",
        "metric '{metric}' uses visibility; the current key is access_modifier",
        "rename the key",
    ),
    _spec(
        "SST-VAL124",
        Severity.WARNING,
        "Duplicate metric expression",
        "metric '{metric}' has the same expression as '{other}'",
        "keep one and synonym the other",
    ),
    _spec(
        "SST-VAL203",
        Severity.ERROR,
        "Relationship names a table not in the view",
        "relationship '{relationship}' names '{name}', absent from {artifact}",
        "add the table to the view, or drop the relationship",
    ),
    _spec(
        "SST-VAL210",
        Severity.WARNING,
        "Relationship target has no matching key",
        "relationship '{relationship}': '{name}' declares neither primary_key nor unique_keys over {value}",
        "declare the key; it is the cheapest fan-out protection",
    ),
    _spec(
        "SST-VAL214",
        Severity.ERROR,
        "Unknown relationship reference",
        "metric '{metric}' names relationship '{relationship}', which is not declared",
        "declare the relationship or correct the name",
    ),
    _spec(
        "SST-VAL223",
        Severity.ERROR,
        "Primary and unique keys overlap",
        "{artifact}: column '{column}' on '{model}' appears in both primary_key and unique_keys",
        "remove it from unique_keys; a primary key is already unique",
    ),
    _spec(
        "SST-VAL305",
        Severity.ERROR,
        "Fact column is not numeric",
        "{artifact}: fact '{member}' has type {found}",
        "use a numeric column, or make it a dimension",
    ),
    _spec(
        "SST-VAL306",
        Severity.ERROR,
        "Time dimension column is not temporal",
        "{artifact}: time_dimension '{member}' has type {found}",
        "use a date or timestamp column, or change column_type",
    ),
    _spec(
        "SST-VAL308",
        Severity.ERROR,
        "Column type metadata is absent",
        "{artifact}: column '{member}' declares no column_type",
        "declare dimension, time_dimension or fact",
    ),
    _spec(
        "SST-VAL309",
        Severity.ERROR,
        "Data type metadata is absent",
        "{artifact}: column '{member}' declares no data_type",
        "declare the Snowflake type",
    ),
    _spec(
        "SST-VAL310",
        Severity.ERROR,
        "Key column is absent",
        "{artifact}: primary_key names '{column}', absent from '{name}'",
        "correct the primary_key or unique_keys list",
    ),
    _spec(
        "SST-VAL312",
        Severity.WARNING,
        "Table declares no key",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys",
        "declare one; cardinality is otherwise guessed from data",
    ),
    _spec(
        "SST-VAL314",
        Severity.ERROR,
        "Enum has no sample values",
        "{artifact}: '{member}' is is_enum and declares no sample_values",
        "populate sample_values or clear is_enum",
    ),
    _spec(
        "SST-VAL315",
        Severity.WARNING,
        "Non-enum sample values look exhaustive",
        "{artifact}: '{member}' declares {count} sample_values and is not is_enum",
        "set is_enum if the set is genuinely closed",
    ),
    _spec(
        "SST-VAL318",
        Severity.ERROR,
        "Excluded column is referenced",
        "{artifact}: '{member}' references excluded column '{column}'",
        "un-exclude the column or change the expression",
    ),
    _spec(
        "SST-VAL401",
        Severity.ERROR,
        "Filter expression is not boolean",
        "filter '{member}' carries labels: [filter] and its expr is not boolean",
        "make the expression boolean",
    ),
    _spec(
        "SST-REF006",
        Severity.ERROR,
        "Metric reference does not resolve",
        "metric('{name}') does not resolve",
        "correct the name or declare the metric",
    ),
    _spec(
        "SST-PRS014",
        Severity.ERROR,
        "Mutually exclusive fields both present",
        "{artifact}: '{field}' and '{other}' are mutually exclusive",
        "declare exactly one",
    ),
    _spec(
        "SST-REF034",
        Severity.ERROR,
        "Legacy table() global is rejected",
        "{file}:{line}:{col}: table('{model}') is not a reference in 1.0; use ref('{model}')",
        "run sst migrate refs",
    ),
    _spec(
        "SST-REF035",
        Severity.ERROR,
        "Legacy column() global is rejected",
        "{file}:{line}:{col}: column('{model}','{column}') is not a reference in 1.0; use ref('{model}','{column}')",
        "run sst migrate refs",
    ),
    _spec(
        "SST-MEM005",
        Severity.WARNING,
        "Member attached to zero artifacts",
        "{member} attaches to no {type}",
        "add its tables to a view, or delete the member",
    ),
    _spec(
        "SST-VAL316",
        Severity.WARNING,
        "Auto-managed field contains a sentinel value",
        "{artifact}: '{member}'.{field} contains '{value}'",
        "re-run sst enrich; the value came from a pandas round-trip",
    ),
    _spec(
        "SST-VAL209",
        Severity.WARNING,
        "Ambiguous join path between two tables",
        "{artifact}: {count} paths between '{a}' and '{b}'",
        "declare using_relationships on the affected metrics",
    ),
    _spec(
        "SST-VAL116",
        Severity.ERROR,
        "using_relationships absent where the join path is ambiguous",
        "metric '{metric}' has {count} paths to '{name}' and declares no using_relationships",
        "declare using_relationships to pick the path",
    ),
    _spec(
        "SST-DBT005",
        Severity.WARNING,
        "Key metadata is written in the 0.3 form",
        "model '{model}': meta.sst.{field} is written in the 0.3 form",
        "write primary_key as a list of columns and unique_keys as a list of column lists; "
        "the 0.3 forms are read for one more release",
    ),
    _spec(
        "SST-DBT030",
        Severity.ERROR,
        "Forbidden meta.sst location key",
        "model '{model}': meta.sst.{key} is forbidden -- delete it",
        "delete the key; relation location comes from dbt's resolved manifest",
    ),
    _spec(
        "SST-CFG041",
        Severity.ERROR,
        "Folder route names a directory that does not exist",
        "config key '{key}' in block '{block}' names no directory under '{root}'",
        "create the directory, or remove the key",
    ),
    _spec(
        "SST-PRT001",
        Severity.ERROR,
        "Snowflake connection failed",
        "connection to {value} failed: {detail}",
        "check credentials, network access, and the selected target",
    ),
    _spec(
        "SST-PRT003",
        Severity.ERROR,
        "Transient Snowflake failure",
        "transient Snowflake failure: {detail}",
        "retry the operation",
    ),
    _spec(
        "SST-PRT004",
        Severity.ERROR,
        "Snowflake privilege refused",
        "Snowflake refused {value}: {detail}",
        "grant the required privilege to the primary role",
    ),
    _spec(
        "SST-PRT005",
        Severity.ERROR,
        "Snowflake object not found",
        "Snowflake object {value} was not found: {detail}",
        "create the dependency or correct its name",
    ),
    _spec("SST-SNO001", Severity.ERROR, "Unrecognised Snowflake refusal", "Snowflake refused: {detail}", None),
    _spec("SST-SNO002", Severity.ERROR, "Object already exists", "{value} already exists", "choose another name"),
    _spec(
        "SST-SNO003",
        Severity.ERROR,
        "Object does not exist or is not authorised",
        "{value} does not exist or is not authorised",
        "publish the object or grant access",
    ),
    _spec(
        "SST-SNO004",
        Severity.ERROR,
        "Insufficient privileges",
        "insufficient privileges for {value}",
        "grant the privilege to the deploying role",
    ),
    _spec(
        "SST-SNO009", Severity.ERROR, "SQL compilation error", "SQL compilation error: {detail}", "fix the statement"
    ),
    _spec(
        "SST-SNO022",
        Severity.ERROR,
        "Concurrent DDL or lock timeout",
        "lock timeout on {value}",
        "retry or serialise publishers",
    ),
    _spec(
        "SST-VAL020",
        Severity.INFO,
        "Connected validation unavailable",
        "connected validation skipped: {detail}",
        "connect to Snowflake to run connected checks",
    ),
    _spec(
        "SST-VAL418",
        Severity.ERROR,
        "Expression does not compile against Snowflake",
        "{type} '{name}': expression failed to compile: {detail}",
        "fix the expression",
    ),
    _spec(
        "SST-PLN001",
        Severity.ERROR,
        "Observation query failed",
        "observation of {value} failed: {detail}",
        "check the connection and role, then re-run plan",
    ),
    _spec(
        "SST-PLN002",
        Severity.ERROR,
        "Different object type exists",
        "{artifact}: {value} exists as a {found}",
        "rename the artifact or remove the conflicting object",
    ),
    _spec(
        "SST-PLN003",
        Severity.WARNING,
        "Prune candidate is unmanaged",
        "{value} has no SST ownership marker; skipped",
        "adopt it explicitly or delete it by hand",
    ),
    _spec(
        "SST-PLN004",
        Severity.WARNING,
        "Prune candidate is absent from state",
        "{value} carries an SST marker and is absent from state; skipped",
        "reconcile state or delete it by hand",
    ),
    _spec(
        "SST-PLN005",
        Severity.ERROR,
        "Artifact dependency cycle",
        "artifact dependency cycle: {cycle}",
        "break the cycle",
    ),
    _spec(
        "SST-PLN013",
        Severity.WARNING,
        "Live object carries explicit grants",
        "{artifact}: {count} explicit grants exist on {value}",
        "confirm COPY GRANTS is emitted before applying",
    ),
    _spec(
        "SST-PLN014",
        Severity.ERROR,
        "Live definition differs from recorded state",
        "{artifact} was changed out of band",
        "review and explicitly reconcile the object before applying",
    ),
    _spec(
        "SST-PLN021",
        Severity.WARNING,
        "Prune reconciliation found undeletable composite resources",
        "{count} objects in {value} are not declared here",
        "retain them until no evaluation run or baseline references them",
    ),
    _spec(
        "SST-PLN023",
        Severity.WARNING,
        "Declared and live case differ",
        "{artifact}: declared '{value}', live object is '{found}'",
        "normalise the declaration to the rendered case",
    ),
    _spec(
        "SST-PLN024",
        Severity.ERROR,
        "Existing object is not managed by SST",
        "{artifact}: {value} exists without trusted SST ownership",
        "adopt or remove the object explicitly before applying",
        demotable=False,
    ),
    _spec(
        "SST-PLN025",
        Severity.ERROR,
        "Artifact target moved",
        "{artifact}: recorded target '{found}' differs from declared target '{expected}'",
        "move the object explicitly, then reconcile state and re-run plan",
        demotable=False,
    ),
    _spec(
        "SST-PLN900",
        Severity.ERROR,
        "Invalid change order",
        "change order violates {value}",
        "report this as a bug",
        demotable=False,
    ),
    _spec(
        "SST-APL001",
        Severity.ERROR,
        "DDL execution failed",
        "{artifact}: {value} failed: {detail}",
        "fix the statement or the account",
    ),
    _spec(
        "SST-APL002",
        Severity.WARNING,
        "Dependency failed",
        "{artifact} skipped: {blocker} failed",
        "fix the dependency and re-apply",
    ),
    _spec(
        "SST-APL003",
        Severity.ERROR,
        "Blocked artifact refused",
        "{artifact} is BLOCKED by {count} errors",
        "fix the errors and re-apply",
    ),
    _spec(
        "SST-APL004",
        Severity.ERROR,
        "Unsafe replacement refused",
        "{artifact}: replace statement omits COPY GRANTS",
        "emit COPY GRANTS; this is not configurable",
        demotable=False,
    ),
    _spec(
        "SST-APL005",
        Severity.ERROR,
        "Plan target differs from apply target",
        "{artifact}: plan target '{found}' differs from apply target '{expected}'",
        "re-run plan against the apply target",
    ),
    _spec(
        "SST-APL006",
        Severity.ERROR,
        "Smoke verification failed",
        "{count} published probes failed",
        "fix the artifacts and rerun the smoke suite",
        demotable=False,
    ),
    _spec(
        "SST-APL008",
        Severity.WARNING,
        "Grants could not be verified",
        "{artifact}: grants could not be verified after replace",
        "check the grants by hand",
    ),
    _spec(
        "SST-APL009",
        Severity.ERROR,
        "Explicit grant was lost",
        "{artifact}: grant {value} was present before replace and is absent after",
        "confirm COPY GRANTS was emitted",
        demotable=False,
    ),
    _spec(
        "SST-APL010",
        Severity.WARNING,
        "Stale apply lock broken",
        "broke a stale lock held by {value}",
        "confirm no other run is live",
    ),
    _spec(
        "SST-APL011",
        Severity.ERROR,
        "Apply lock is held",
        "{value} holds the apply lock",
        "wait for the other run or explicitly break a stale lock",
    ),
    _spec(
        "SST-APL012",
        Severity.ERROR,
        "Object changed after plan",
        "{artifact}: {value} changed since the plan",
        "re-run plan",
    ),
    _spec(
        "SST-APL016",
        Severity.ERROR,
        "Partial write left the artifact in an unusable state",
        "{artifact}: {detail}",
        "re-apply after reconciling the recorded physical resources",
        demotable=False,
    ),
    _spec(
        "SST-APL022",
        Severity.ERROR,
        "Dataset publication failed",
        "dataset '{artifact}': {detail}",
        "check source-table and dataset privileges, then re-apply",
        demotable=False,
    ),
    _spec(
        "SST-APL023",
        Severity.ERROR,
        "Eval run could not be started",
        "eval '{artifact}': {detail}",
        "check CREATE TASK, CREATE STAGE and the file format",
        demotable=False,
    ),
    _spec(
        "SST-APL024",
        Severity.WARNING,
        "Eval run reported a partial terminal status",
        "eval '{artifact}': status '{found}'",
        "treat this as a failure and re-run",
    ),
    _spec(
        "SST-APL028",
        Severity.ERROR,
        "Eval config stage has the wrong FILE FORMAT",
        "eval config stage '{value}': FILE FORMAT is {found}, expected {expected}",
        "alter the stage once to the required format; SST will not alter it",
        demotable=False,
    ),
    _spec(
        "SST-APL100",
        Severity.ERROR,
        "Smoke probe failed",
        "{artifact}: smoke probe failed: {detail}",
        "fix the artifact and rerun the smoke suite",
        demotable=False,
    ),
    _spec(
        "SST-APL900",
        Severity.ERROR,
        "Apply outcome count mismatch",
        "applied {found} outcomes for {expected} changes",
        "report this as a bug",
        demotable=False,
    ),
    _spec("SST-MAN001", Severity.ERROR, "Manifest missing", "no SST manifest at {path}", "run sst compile"),
    _spec(
        "SST-MAN002",
        Severity.ERROR,
        "Manifest is invalid JSON",
        "{path} is not valid JSON: {detail}",
        "delete it and re-run sst compile",
    ),
    _spec(
        "SST-MAN003",
        Severity.ERROR,
        "Manifest key missing",
        "{path} omits required key '{key}'",
        "re-run sst compile",
    ),
    _spec(
        "SST-MAN004",
        Severity.ERROR,
        "Impact index incomplete",
        "{artifact} has no reverse-index entry",
        "re-run sst compile",
    ),
    _spec(
        "SST-MAN005",
        Severity.ERROR,
        "Manifest content hash mismatch",
        "manifest_id {found}, recomputed {expected}",
        "re-run sst compile",
    ),
    _spec(
        "SST-MAN020",
        Severity.WARNING,
        "State is absent",
        "no state file; treating every artifact as new",
        "check that the state table is readable",
    ),
    _spec(
        "SST-MAN021",
        Severity.WARNING,
        "State and manifest differ",
        "state was recorded against manifest {found}; current is {expected}",
        "re-run plan; NOOP detection is disabled",
    ),
    _spec(
        "SST-MAN022",
        Severity.ERROR,
        "State is unreadable",
        "{path} is present and unreadable: {detail}",
        "fix or explicitly delete the file",
    ),
    _spec(
        "SST-MAN023",
        Severity.ERROR,
        "State schema unsupported",
        "{path} declares schema {found}",
        "upgrade SST or explicitly clear the state",
    ),
    _spec(
        "SST-MAN027",
        Severity.WARNING,
        "State cache disagrees with remote state",
        "local state.json for target {value} disagrees with {detail}; the table wins",
        "the run proceeds from authoritative remote state",
    ),
    _spec(
        "SST-MAN201",
        Severity.INFO,
        "Manifest migrated in memory",
        "{path} schema {found} migrated to {expected} in memory",
        None,
    ),
    _spec(
        "SST-MAN202",
        Severity.WARNING,
        "Manifest schema has no migration",
        "{path} schema {found} has no migration; full recompile",
        "re-run sst compile",
    ),
    _spec(
        "SST-MAN203",
        Severity.ERROR,
        "Manifest schema is newer",
        "{path} schema {found}; this binary supports {expected}",
        "upgrade SST",
    ),
    _spec(
        "SST-INT900",
        Severity.ERROR,
        "Unregistered diagnostic code",
        "unregistered code {value}",
        "report this as a bug",
        demotable=False,
    ),
    _spec(
        "SST-INT901",
        Severity.ERROR,
        "Missing diagnostic context",
        "{value} template needs {placeholder}, which was not supplied",
        "report this as a bug",
        demotable=False,
    ),
    _spec(
        "SST-INT902",
        Severity.ERROR,
        "Domain invariant violated",
        "domain invariant violated: {detail}",
        "report this as a bug",
        demotable=False,
    ),
)

_PARITY_SPECS = (
    _spec(
        "SST-PRS004",
        Severity.WARNING,
        "Unknown field in a known block",
        "{artifact}: unknown field '{field}'",
        "remove the field, or check the spelling",
    ),
    _spec(
        "SST-PRS010",
        Severity.ERROR,
        "Identifier exceeds the object name limit",
        "'{name}' is {size} chars, over the {expected} limit",
        "shorten the name",
    ),
    _spec(
        "SST-PRS015",
        Severity.ERROR,
        "Paired fields not declared together",
        "{artifact}: '{field}' requires '{other}'",
        "declare both, or neither",
    ),
    _spec(
        "SST-PRS016",
        Severity.ERROR,
        "Numeric value is outside its allowed range",
        "{artifact}: '{field}' is {found}, expected {expected}",
        "use a value within the allowed range",
    ),
    _spec(
        "SST-PRS025",
        Severity.ERROR,
        "Reserved agent alias used",
        "{artifact}: alias '{value}' is reserved",
        "choose a non-reserved alias",
    ),
    _spec(
        "SST-PRS033",
        Severity.ERROR,
        "Required input is not a declared property",
        "{artifact}: input_schema.required names '{field}', absent from properties",
        "declare the property, or remove it from required",
    ),
    _spec(
        "SST-PRS114",
        Severity.ERROR,
        "Score ranges are not contiguous",
        "{artifact}: score_ranges leave a gap or overlap at {value}",
        "make the ranges contiguous with min-inclusive, max-inclusive boundaries",
    ),
    _spec(
        "SST-PRS115",
        Severity.ERROR,
        "Custom metric threshold has no usable bound",
        "{artifact}: threshold_default {found} is not a usable bound within max_score {expected}",
        "declare min, optionally max, with min <= max and both inside max_score",
    ),
    _spec(
        "SST-PRS116",
        Severity.ERROR,
        "Unsupported template placeholder in a judge prompt",
        "{artifact}: '{placeholder}' is not one of the 12 supported names",
        "use a supported placeholder",
    ),
    _spec(
        "SST-PRS117",
        Severity.ERROR,
        "Dataset row has no question or no expected field",
        "{artifact}: row {index} has {detail}",
        "give every row a question and at least one expectation",
    ),
    _spec(
        "SST-PRS118",
        Severity.ERROR,
        "Sample question entry has the wrong shape",
        "{artifact}: sample_questions[{index}] is a string, expected a mapping",
        "use {question: ...} mappings",
    ),
    _spec(
        "SST-REF011",
        Severity.ERROR,
        "Semantic-view reference does not resolve",
        "{{{{ semantic_view('{name}') }}}} does not resolve",
        "declare the view, or correct the reference",
    ),
    _spec(
        "SST-REF012",
        Severity.ERROR,
        "Agent reference does not resolve",
        "{{{{ agent('{name}') }}}} does not resolve",
        "declare the agent, or correct the reference",
    ),
    _spec(
        "SST-REF013",
        Severity.ERROR,
        "Extension reference does not resolve",
        "{{{{ extension('{name}') }}}} does not resolve",
        "declare the extension, or correct the reference",
    ),
    _spec(
        "SST-REF014",
        Severity.ERROR,
        "File sidecar reference does not resolve",
        "{{{{ file('{path}') }}}} does not resolve to a file",
        "create the sidecar, or correct the path",
    ),
    _spec(
        "SST-REF022",
        Severity.ERROR,
        "Agent delegation graph has a cycle",
        "agent delegation cycle: {cycle}",
        "break the delegation cycle",
    ),
    _spec(
        "SST-REF026",
        Severity.ERROR,
        "Eval metric reference does not resolve",
        "{{{{ eval_metric('{name}') }}}} does not resolve",
        "declare the metric in the eval_metrics/ tree, or correct the name",
    ),
    _spec(
        "SST-REF027",
        Severity.ERROR,
        "File sidecar escapes the project root",
        "{{{{ file('{path}') }}}} resolves outside the project root",
        "use a path inside the project",
    ),
    _spec(
        "SST-RND012",
        Severity.ERROR,
        "Unknown agent tool type",
        "agent '{artifact}': tool type '{found}' is unknown to the renderer",
        "use a supported type, or extend the allowlist",
    ),
    _spec(
        "SST-VAL511",
        Severity.ERROR,
        "Rendered agent spec exceeds the size limit",
        "agent '{artifact}': rendered spec is {size} bytes, over the 100,000 limit",
        "trim the spec or move bulk context out of it",
    ),
    _spec(
        "SST-VAL512",
        Severity.WARNING,
        "Rendered agent spec is near the size limit",
        "agent '{artifact}': rendered spec is {size} bytes, over 80% of the limit",
        "trim the specification before it reaches the hard limit",
    ),
    _spec(
        "SST-VAL513",
        Severity.ERROR,
        "Resolved agent tool name is invalid",
        "agent '{artifact}': resolved tool name '{name}' is {size} chars",
        "use a 1-64 character name",
    ),
    _spec(
        "SST-VAL514",
        Severity.ERROR,
        "Agent tool names collide",
        "agent '{artifact}': tool name '{name}' is declared twice",
        "rename one tool",
    ),
    _spec(
        "SST-VAL515",
        Severity.WARNING,
        "Agent tool names differ only by case",
        "agent '{artifact}': '{a}' and '{b}' differ only by case",
        "rename one tool",
    ),
    _spec(
        "SST-VAL517",
        Severity.ERROR,
        "Web search tool has a non-canonical name",
        "agent '{artifact}': web_search tool is named '{name}'",
        "name it web_search",
    ),
    _spec(
        "SST-VAL518",
        Severity.ERROR,
        "Agent tool has no description",
        "agent '{artifact}': tool '{name}' has no description",
        "write a disambiguating tool description",
    ),
    _spec(
        "SST-VAL520",
        Severity.ERROR,
        "Analyst tool has an invalid semantic-view declaration",
        "agent '{artifact}': tool '{name}' declares {count} semantic views",
        "declare exactly one semantic_view and omit name",
    ),
    _spec(
        "SST-VAL521",
        Severity.ERROR,
        "Agent tool omits a required resource",
        "agent '{artifact}': tool '{name}' omits '{field}'",
        "declare the required resource reference",
    ),
    _spec(
        "SST-VAL526",
        Severity.ERROR,
        "Generic tool has no object input schema",
        "agent '{artifact}': tool '{name}' input_schema is {found}",
        "declare input_schema with type object",
    ),
    _spec(
        "SST-VAL527",
        Severity.ERROR,
        "Generic tool has no warehouse",
        "agent '{artifact}': tool '{name}' declares no warehouse",
        "declare or inherit a warehouse",
    ),
    _spec(
        "SST-VAL528",
        Severity.WARNING,
        "Agent toolset makes the tool surface non-static",
        "agent '{artifact}' declares an agent toolset",
        "account for delegated tools in tests",
    ),
    _spec(
        "SST-VAL538",
        Severity.ERROR,
        "Skill source is not pinned",
        "agent '{artifact}': skill source '{name}' does not pin an immutable version",
        "pin a committed extension version",
    ),
    _spec(
        "SST-VAL539",
        Severity.ERROR,
        "Skill source uses a mutable stage",
        "agent '{artifact}': skill source '{name}' is a STAGE path into a mutable bundle",
        "reference a versioned Cortex Extension",
    ),
    _spec(
        "SST-VAL543",
        Severity.ERROR,
        "Orchestration model is not allowed",
        "agent '{artifact}': models.orchestration '{found}' is not in the allowlist",
        "use an allowed model, or extend the allowlist",
    ),
    _spec(
        "SST-VAL545",
        Severity.ERROR,
        "tool_not_accessible is invalid",
        "agent '{artifact}': tool_not_accessible {detail}",
        "use accept, reject, or legacy",
    ),
    _spec(
        "SST-VAL546",
        Severity.ERROR,
        "Analytical search has no Cortex Search tool",
        "agent '{artifact}': analytical_search is true and no cortex_search tool is declared",
        "declare a Cortex Search tool, or disable the capability",
    ),
    _spec(
        "SST-VAL549",
        Severity.WARNING,
        "Agent display name collides",
        "agent '{artifact}': display_name '{value}' is shared with {other}",
        "give each agent a distinct display name",
    ),
    _spec(
        "SST-VAL701",
        Severity.ERROR,
        "Dataset name is not unique within the agent schema",
        "dataset '{artifact}' is declared twice in {value}",
        "rename one of them",
    ),
    _spec(
        "SST-VAL702",
        Severity.ERROR,
        "Dataset name exceeds the object limit",
        "dataset '{artifact}' is {size} chars, over 128",
        "shorten the name",
    ),
    _spec(
        "SST-VAL703",
        Severity.ERROR,
        "Dataset declares its own database or schema",
        "dataset '{artifact}' declares +{key}",
        "remove it; eval objects resolve to the agent's schema",
    ),
    _spec(
        "SST-VAL705",
        Severity.ERROR,
        "Dataset row is incomplete",
        "dataset '{artifact}': row {index} {detail}",
        "give every row a question and at least one expectation",
    ),
    _spec(
        "SST-VAL706",
        Severity.ERROR,
        "Relative date in an eval question or expected answer",
        "dataset '{artifact}': row {index} contains relative date '{value}'",
        "pin the date",
    ),
    _spec(
        "SST-VAL707",
        Severity.WARNING,
        "Eval question duplicates an agent sample question",
        "dataset '{artifact}': row {index} is byte-identical to a sample_question",
        "choose questions that would catch the agent failing",
    ),
    _spec(
        "SST-VAL708",
        Severity.ERROR,
        "ground_truth_invocations names a tool the agent does not have",
        "dataset '{artifact}': row {index} expects tool '{name}', absent from agent '{value}'",
        "correct the tool name, or add the tool",
    ),
    _spec(
        "SST-VAL709",
        Severity.ERROR,
        "web_search expectation uses a non-canonical name",
        "dataset '{artifact}': row {index} expects '{name}'",
        "use the literal name web_search",
    ),
    _spec(
        "SST-VAL710",
        Severity.WARNING,
        "Dataset has fewer rows than the configured floor",
        "dataset '{artifact}' has {count} rows, under {expected}",
        "add rows; a thin dataset cannot support range thresholds",
    ),
    _spec(
        "SST-VAL711",
        Severity.INFO,
        "Question-set tool coverage",
        "dataset '{artifact}': {value} of the agent's tools are exercised by no question",
        None,
    ),
    _spec(
        "SST-VAL712",
        Severity.INFO,
        "CREATE DATASET takes no properties",
        "dataset '{artifact}': versions and provenance are added by ALTER, not CREATE",
        None,
    ),
    _spec(
        "SST-VAL717",
        Severity.ERROR,
        "run_name is not unique for the agent",
        "run_name '{value}' is already used for {artifact}",
        "choose a distinct run_name",
    ),
    _spec(
        "SST-VAL718",
        Severity.ERROR,
        "run_name carries no commit SHA",
        "run_name '{value}' does not include the git SHA",
        "include the SHA so a score is attributable to a commit",
    ),
    _spec(
        "SST-VAL719",
        Severity.ERROR,
        "agent_version is LIVE or omitted",
        "eval config for '{artifact}': agent_version is '{found}'",
        "use committed, alias:<name>, or VERSION$<integer>",
    ),
    _spec(
        "SST-VAL721",
        Severity.ERROR,
        "Metric is neither a system metric nor a declared custom metric",
        "eval config for '{artifact}': metric '{name}' is unknown",
        "declare the custom metric, or use a system metric",
    ),
    _spec(
        "SST-VAL722",
        Severity.ERROR,
        "metric_version is not pinned",
        "eval config for '{artifact}': metric '{name}' declares no metric_version",
        "pin the system metric to v3",
    ),
    _spec(
        "SST-VAL723",
        Severity.WARNING,
        "metric_version pins a legacy judge",
        "eval config for '{artifact}': metric '{name}' pins legacy version '{found}'",
        "move to v3, and record the score shift",
    ),
    _spec(
        "SST-VAL724",
        Severity.ERROR,
        "judge_model set for a system metric",
        "eval config for '{artifact}': metric '{name}' is a system metric and declares judge_model",
        "remove judge_model; the version carries the judge",
    ),
    _spec(
        "SST-VAL725",
        Severity.INFO,
        "Metric evaluation mechanics",
        "eval config for '{artifact}': metric '{name}' {detail}",
        None,
    ),
    _spec(
        "SST-VAL726",
        Severity.WARNING,
        "logical_consistency used as a gate",
        "eval config for '{artifact}': logical_consistency is a blocking metric",
        "report it rather than gate on it",
    ),
    _spec(
        "SST-VAL731",
        Severity.WARNING,
        "Eval concurrency exceeds the configured ceiling",
        "eval config for '{artifact}': concurrency {found} exceeds {expected}",
        "lower concurrency; runs re-invoke the agent",
    ),
    _spec(
        "SST-VAL732",
        Severity.INFO,
        "Unsupported tool types are skipped during evals",
        "eval config for '{artifact}': {value} will be skipped, not failed",
        None,
    ),
    _spec(
        "SST-VAL733",
        Severity.ERROR,
        "Blocking threshold declares no usable bound",
        "eval config for '{artifact}': metric '{name}' threshold has {detail}",
        "declare min, max, or both, with min <= max",
    ),
    _spec(
        "SST-VAL735",
        Severity.WARNING,
        "Threshold set with no baseline",
        "eval config for '{artifact}': metric '{name}' has a threshold and no baseline",
        "measure first, then set the threshold",
    ),
    _spec(
        "SST-VAL758",
        Severity.ERROR,
        "Eval baseline is absent",
        "eval '{artifact}' has no captured baseline",
        "capture a baseline explicitly with a reason",
    ),
    _spec(
        "SST-VAL759",
        Severity.ERROR,
        "Eval baseline is incompatible",
        "eval '{artifact}' baseline is incompatible: {detail}",
        "capture a new baseline for the current dataset, agent version and metrics",
    ),
    _spec(
        "SST-VAL760",
        Severity.WARNING,
        "Eval baseline is nearing expiry",
        "eval '{artifact}' baseline expires on {date}",
        "capture a replacement baseline before it expires",
    ),
    _spec(
        "SST-VAL761",
        Severity.ERROR,
        "Eval baseline is expired",
        "eval '{artifact}' baseline expired on {date}",
        "capture a replacement baseline with a reason",
        demotable=False,
    ),
    _spec(
        "SST-VAL737",
        Severity.ERROR,
        "Custom metric shadows a system metric name",
        "eval metric '{artifact}' shadows system metric '{name}'",
        "rename the custom metric",
    ),
    _spec(
        "SST-VAL738",
        Severity.ERROR,
        "Custom metric declares no explicit judge model",
        "eval metric '{artifact}': judge_model is '{found}'",
        "name the judge model explicitly",
    ),
    _spec(
        "SST-VAL739",
        Severity.ERROR,
        "judge_model is not in the allowlist",
        "eval metric '{artifact}': judge_model '{found}' is not in the allowlist",
        "add it to the config allowlist, or use an allowed model",
    ),
    _spec(
        "SST-VAL740",
        Severity.WARNING,
        "Custom metric duplicates a system metric's intent",
        "eval metric '{artifact}' duplicates '{name}'",
        "use the system metric; it is cheaper, versioned and comparable",
    ),
    _spec(
        "SST-VAL741",
        Severity.ERROR,
        "Judge prompt declares no output contract",
        "eval metric '{artifact}': the prompt declares no scale or allowed values",
        "state the scale and the allowed values in the prompt",
    ),
    _spec(
        "SST-VAL742",
        Severity.WARNING,
        "Judge prompt asks for reasoning with no parseable score",
        "eval metric '{artifact}': the prompt has no parseable score instruction",
        "ask for a number in a fixed position",
    ),
    _spec(
        "SST-VAL743",
        Severity.WARNING,
        "Rubric has no tie-break or insufficient-information branch",
        "eval metric '{artifact}': the rubric has no {detail} branch",
        "add the branch; otherwise the judge invents one per call",
    ),
    _spec(
        "SST-VAL747",
        Severity.ERROR,
        "Scoring instruction produces a number outside the declared ranges",
        "eval metric '{artifact}': the prompt can produce {found}, outside {expected}",
        "align the prompt with score_ranges",
    ),
    _spec(
        "SST-VAL748",
        Severity.WARNING,
        "Judged range is wider than three bands for an invariant",
        "eval metric '{artifact}' declares {count} bands for an invariant",
        "narrow the scale",
    ),
    _spec(
        "SST-VAL601",
        Severity.ERROR,
        "Tool member is duplicated within its group",
        "tool group '{a}': member '{name}' is declared twice",
        "rename or remove one declaration",
    ),
    _spec(
        "SST-VAL602",
        Severity.WARNING,
        "Tool member name is not globally unique",
        "tool member '{name}' is declared in {a} and {b}",
        "use the canonical two-argument tool reference",
    ),
    _spec(
        "SST-VAL603",
        Severity.ERROR,
        "Unknown tool type",
        "tool member '{name}': type '{found}' is not a known tool type",
        "use one of: {expected}",
    ),
    _spec(
        "SST-VAL604",
        Severity.ERROR,
        "Defined tool lacks creation properties",
        "tool member '{name}' is under define: and declares neither on: nor body_file:",
        "add the creation fields required by the tool type",
    ),
    _spec(
        "SST-VAL605",
        Severity.ERROR,
        "Tool ownership category is inconsistent",
        "tool member '{name}' is under reference: and declares '{key}'",
        "move creation properties under define:, or remove them",
    ),
    _spec(
        "SST-VAL606",
        Severity.ERROR,
        "Immutable tool group contains managed objects",
        "tool group '{a}' is immutable: true and declares define:",
        "remove define:, or make the group mutable",
    ),
    _spec(
        "SST-VAL608",
        Severity.ERROR,
        "Tool source is not a dbt model",
        "tool member '{name}': on: '{value}' is not a model in the dbt manifest",
        "reference a dbt model in the current manifest",
    ),
    _spec(
        "SST-VAL609",
        Severity.ERROR,
        "Tool column is absent from its source",
        "tool member '{name}': '{column}' is not on '{value}'",
        "use a column present on the source model",
    ),
    _spec(
        "SST-REF010",
        Severity.ERROR,
        "Tool reference does not resolve",
        "{{{{ tool('{group}','{name}') }}}} does not resolve",
        "declare the group and member, or correct the reference",
    ),
    _spec(
        "SST-REF018",
        Severity.ERROR,
        "Reference has no current-target location",
        "{{{{ {ref_function}('{name}') }}}} has no entry for target '{target}'",
        "add an explicit relation for the current dbt target",
    ),
    _spec(
        "SST-REF019",
        Severity.ERROR,
        "Referenced object name is not fully qualified",
        "'{value}' is not a three-part fully-qualified name",
        "use DATABASE.SCHEMA.OBJECT",
    ),
    _spec(
        "SST-REF020",
        Severity.ERROR,
        "Referenced object type is incompatible",
        "{{{{ {ref_function}('{name}') }}}} resolves to {found}, expected {expected}",
        "point the reference at a compatible object type",
    ),
    _spec(
        "SST-REF023",
        Severity.WARNING,
        "Multiple targets resolve to one writable object",
        "{{{{ {ref_function}('{name}') }}}} resolves to {value} for both dev and prod",
        "map each writable target explicitly, or mark the group immutable",
    ),
    _spec(
        "SST-PRS032",
        Severity.ERROR,
        "Tool input uses unsupported object type",
        "{artifact}: input_schema property '{field}' has type '{found}'",
        "use a supported scalar or array type",
    ),
    _spec(
        "SST-DBT002",
        Severity.ERROR,
        "Referenced dbt model is absent",
        "model '{model}' is not in the dbt manifest",
        "run dbt compile, or correct the name",
    ),
    _spec(
        "SST-DBT003",
        Severity.ERROR,
        "Unknown dbt semantic role",
        "model '{model}': meta.sst role '{found}' is not a known role",
        "correct the role name",
    ),
    _spec(
        "SST-DBT004",
        Severity.WARNING,
        "dbt and semantic column types disagree",
        "model '{model}': column '{column}' is {found} in dbt and {expected} in the semantic layer",
        "reconcile the two types, or add a dbt contract",
    ),
    _spec(
        "SST-LOD018",
        Severity.ERROR,
        "Referenced sidecar is missing",
        "{file} references {path}, which does not exist",
        "create the file, or correct the path",
    ),
    _spec(
        "SST-LOD019",
        Severity.ERROR,
        "Referenced sidecar is empty",
        "{path}, referenced by {file}, is empty",
        "add content, or remove the reference",
    ),
    _spec(
        "SST-MEM002",
        Severity.WARNING,
        "Member tables cannot be inferred",
        "{member} declares no tables: and none can be inferred",
        "declare tables explicitly",
    ),
    _spec(
        "SST-MEM008",
        Severity.ERROR,
        "Member reaches a table absent from its artifact",
        "{member} attaches to {artifact}, which lacks table '{name}'",
        "align the member tables with the view",
    ),
    _spec(
        "SST-MEM011",
        Severity.INFO,
        "Member fan-out",
        "{member} attaches to {count} artifacts",
        None,
    ),
    _spec(
        "SST-MEM103",
        Severity.INFO,
        "Artifact member counts",
        "{artifact}: {value}",
        None,
    ),
    _spec(
        "SST-PRS006",
        Severity.ERROR,
        "Duplicate name within a type",
        "{type} '{name}' is declared more than once",
        "rename one declaration",
    ),
    _spec(
        "SST-PRS018",
        Severity.ERROR,
        "Collection element has the wrong shape",
        "{artifact}: '{field}'[{index}] expects {expected}, found {found}",
        "correct the element",
    ),
    _spec(
        "SST-PRS019",
        Severity.ERROR,
        "Boolean field holds a non-boolean",
        "{artifact}: '{field}' is '{found}', expected a boolean",
        "use true or false",
    ),
    _spec(
        "SST-PRS027",
        Severity.ERROR,
        "Tags block has the wrong shape",
        "{artifact}: tags must be a mapping of name to value, found {found}",
        "correct the tags block",
    ),
    _spec(
        "SST-PRS028",
        Severity.ERROR,
        "Constraints block has the wrong shape",
        "{artifact}: constraints block is invalid: {detail}",
        "correct the constraints block",
    ),
    _spec(
        "SST-PRS101",
        Severity.ERROR,
        "Required collection is empty",
        "{artifact}: '{field}' is present and empty",
        "declare at least one entry, or remove the key",
    ),
    _spec(
        "SST-PRS106",
        Severity.ERROR,
        "Duplicate member name within an owner",
        "{artifact}: {member_type} '{name}' is declared twice",
        "rename one member",
    ),
    _spec(
        "SST-PRS107",
        Severity.ERROR,
        "Member declares no name",
        "{artifact}: {member_type} entry {index} has no name",
        "name the member",
    ),
    _spec(
        "SST-VAL010",
        Severity.ERROR,
        "Reference graph contains a cycle",
        "{type} reference cycle: {cycle}",
        "break the cycle",
    ),
    _spec(
        "SST-VAL012",
        Severity.WARNING,
        "Deprecated key spelling in use",
        "{type} '{name}' uses '{field}'; the current spelling is '{expected}'",
        "rename the key",
    ),
    _spec(
        "SST-VAL118",
        Severity.ERROR,
        "Non-additive dimension does not resolve",
        "metric '{metric}': non_additive_dimensions names {value}, which does not resolve",
        "correct the table and dimension names",
    ),
    _spec(
        "SST-VAL201",
        Severity.ERROR,
        "Relationship declares no conditions",
        "relationship '{relationship}' declares no conditions",
        "declare at least one condition",
    ),
    _spec(
        "SST-VAL204",
        Severity.ERROR,
        "Relationship column is on the wrong table",
        "relationship '{relationship}': '{column}' is not on '{name}'",
        "correct the column, or swap the sides",
    ),
    _spec(
        "SST-VAL205",
        Severity.ERROR,
        "Relationship sides share no view",
        "relationship '{relationship}' joins '{a}' and '{b}', which share no view",
        "add both tables to one view, or drop the relationship",
    ),
    _spec(
        "SST-VAL213",
        Severity.ERROR,
        "Relationship condition is not expressible",
        "relationship '{relationship}': condition '{value}' spans multiple columns per side",
        "split it into one condition per column pair",
    ),
    _spec(
        "SST-VAL215",
        Severity.ERROR,
        "Relationship graph cycle changes results",
        "{artifact}: relationship cycle {cycle}",
        "break the cycle, or split role-playing tables",
    ),
    _spec(
        "SST-VAL311",
        Severity.ERROR,
        "Required primary key is absent",
        "{artifact}: '{name}' declares no primary_key",
        "declare primary_key in config.meta.sst",
    ),
    _spec(
        "SST-VAL403",
        Severity.ERROR,
        "Legacy inline filter syntax",
        "filter '{member}' uses the legacy inline form",
        "declare filters as named objects with labels",
        demotable=False,
    ),
    _spec(
        "SST-VAL412",
        Severity.ERROR,
        "Verified-query SQL source is invalid",
        "verified_query '{member}': {detail}",
        "declare exactly one of sql or sql_file",
    ),
    _spec(
        "SST-VAL413",
        Severity.WARNING,
        "Verified-query SQL reads an undeclared table",
        "verified_query '{member}': its SQL reads '{name}', which is not in its tables:",
        "add the table to tables:, which decides the views the query attaches to",
    ),
)


# Configuration shape, skills, plugins, and Desktop profiles.
_PUBLISHING_SPECS = (
    _spec(
        "SST-CFG003",
        Severity.WARNING,
        "Unknown config key",
        "unknown config key '{key}'",
        "remove the key, or check it against the configuration reference",
    ),
    _spec(
        "SST-CFG004",
        Severity.ERROR,
        "Config value has the wrong type",
        "config key '{key}' expects {expected}, found {found}",
        "correct the value type",
    ),
    _spec(
        "SST-CFG006",
        Severity.ERROR,
        "Required config key missing",
        "required config key '{key}' is absent",
        "add the key to sst_config.yml",
    ),
    _spec(
        "SST-CFG007",
        Severity.ERROR,
        "Unknown top-level key near a known key",
        "unknown top-level key '{key}'; did you mean '{suggestion}'?",
        "correct the spelling",
    ),
    _spec(
        "SST-CFG008",
        Severity.ERROR,
        "Config value outside its allowed domain",
        "config key '{key}' value {found} is outside {expected}",
        "use an allowed value",
    ),
    _spec(
        "SST-CFG015",
        Severity.ERROR,
        "evals block declares a database or schema",
        "evals: declares {key}, which is structurally invalid",
        "remove +database and +schema from evals:; eval objects resolve to the agent's schema",
    ),
    _spec(
        "SST-CFG040",
        Severity.ERROR,
        "sha_version is declared in vars",
        "vars.sha_version is supplied by SST and must not be declared",
        "remove it from vars:",
    ),
    _spec(
        "SST-CFG042",
        Severity.ERROR,
        "Folder route declared under evals or skills",
        "{block}: declares a folder route '{key}'",
        "remove it; evals: location is structural, and skills: keeps only its catalog, stage, and extensions blocks",
    ),
    _spec(
        "SST-CFG043",
        Severity.ERROR,
        "Config key was removed",
        "config key '{key}' was removed: {reason}",
        "delete the key",
    ),
    _spec(
        "SST-CFG044",
        Severity.INFO,
        "Config key has no effect",
        "config key '{key}' is accepted for compatibility and has no effect in this release",
        "delete the key, or keep it only while 0.3 still reads this file",
    ),
    _spec(
        "SST-CFG045",
        Severity.WARNING,
        "Deprecated config key",
        "config key '{key}' is deprecated; use '{replacement}'",
        "rename the key",
    ),
    _spec(
        "SST-CFG046",
        Severity.ERROR,
        "Configuration requires a dbt project",
        "{key} requires a dbt project, and the project has no dbt_project.yml",
        "add dbt_project.yml, or remove the configuration; a project without dbt publishes skills, plugins, "
        "and profiles only",
    ),
    _spec(
        "SST-VAL817",
        Severity.ERROR,
        "No publication channel configured",
        "{key}: configures neither the catalog nor the stage channel",
        "configure skills.catalog, skills.stage, or both",
    ),
    _spec(
        "SST-VAL818",
        Severity.ERROR,
        "Flatten setting is wrong for its channel",
        "config key '{key}' is {found}; it must be {expected}",
        "set skills.catalog.+flatten true and skills.stage.+flatten false",
    ),
    _spec(
        "SST-VAL819",
        Severity.ERROR,
        "Stage auto_compress is enabled",
        "config key '{key}' is {found}; it must be {expected}",
        "set it false; compressed uploads are invisible to Desktop",
    ),
    _spec(
        "SST-VAL821",
        Severity.ERROR,
        "Stage layout is not by_type",
        "config key '{key}' value {found} is outside {expected}",
        "set skills.stage.+layout to by_type",
    ),
    _spec(
        "SST-PRS034",
        Severity.ERROR,
        "Frontmatter missing a required key",
        "{artifact}: SKILL.md frontmatter omits '{field}'",
        "declare name and description in the frontmatter",
    ),
    _spec(
        "SST-PRS119",
        Severity.ERROR,
        "Skill folder contains another skill folder",
        "{artifact}: nested skill folder at {path}",
        "move the nested skill beside its parent; one skill per folder",
    ),
    _spec(
        "SST-RND031",
        Severity.WARNING,
        "Skill body is empty",
        "skill '{artifact}': SKILL.md has no instructions after its frontmatter",
        "add the instructions the skill carries",
    ),
    _spec(
        "SST-VAL801",
        Severity.ERROR,
        "Skill folder layout or name is wrong",
        "'{artifact}': {detail}",
        "name the folder in lowercase kebab-case and repeat that name in the frontmatter",
    ),
    _spec(
        "SST-VAL808",
        Severity.ERROR,
        "Referenced bundle path does not resolve",
        "skill '{artifact}': '{path}' is referenced and absent from the bundle",
        "add the file, correct the reference, or mark an illustrative line with 'sst: ignore SST-VAL808'",
    ),
    _spec(
        "SST-VAL809",
        Severity.ERROR,
        "Flattened file names collide",
        "skill '{artifact}': '{a}' and '{b}' flatten to one name",
        "rename one of the authored files",
    ),
    _spec(
        "SST-VAL810",
        Severity.ERROR,
        "Reference does not survive flattening",
        "skill '{artifact}': '{path}' does not resolve inside the published bundle",
        "reference bundled files relative to the skill folder; repository-root paths and directories are not published",
    ),
    _spec(
        "SST-VAL811",
        Severity.WARNING,
        "Flattened bundle exceeds the size budget",
        "'{artifact}': bundle is {size} bytes, over {expected}",
        "move bulk content out of the bundle; it is read on demand at invocation",
    ),
    _spec(
        "SST-VAL812",
        Severity.WARNING,
        "SKILL.md exceeds the size budget",
        "skill '{artifact}': SKILL.md is {size} bytes, over {expected}",
        "move bulk content into a referenced file; SKILL.md is read on every orchestration turn",
    ),
    _spec(
        "SST-VAL813",
        Severity.WARNING,
        "Bundled file is never referenced",
        "skill '{artifact}': '{path}' is never referenced from the skill's Markdown",
        "reference it, or remove it",
    ),
    _spec(
        "SST-VAL815",
        Severity.WARNING,
        "Bundled file reads a credential or an absolute local path",
        "skill '{artifact}': '{path}' contains {detail}",
        "parameterise it; a path that resolves on one machine does not resolve in the sandbox",
    ),
    _spec(
        "SST-VAL832",
        Severity.ERROR,
        "Skill or plugin name is not unique as an extension",
        "'{artifact}' collides with '{other}' as one extension name",
        "rename one of them",
    ),
    _spec(
        "SST-VAL833",
        Severity.WARNING,
        "Script names a path that flattening renames",
        "skill '{artifact}': '{path}' names '{target}', which the catalog bundle renames",
        "keep files a script reads beside SKILL.md, or pass the path in as an argument",
    ),
    _spec(
        "SST-VAL834",
        Severity.ERROR,
        "Bundle exceeds the extension scan limits",
        "'{artifact}': {detail}",
        "split the bundle; Snowflake scans at most 50 files, 2 MiB per file, 10 MiB per version",
    ),
    _spec(
        "SST-VAL835",
        Severity.ERROR,
        "Plugin member is not a project skill",
        "plugin '{artifact}': member '{name}' is not a skill in this project",
        "add the skill under the skills directory, or remove it from the plugin",
    ),
    _spec(
        "SST-VAL836",
        Severity.ERROR,
        "Plugin member cannot be bundled",
        "plugin '{artifact}': member '{name}' has errors, so the plugin cannot be bundled",
        "fix the member skill's errors",
    ),
    _spec(
        "SST-VAL837",
        Severity.WARNING,
        "Plugin lists a member twice",
        "plugin '{artifact}': lists '{name}' more than once",
        "list each member once",
    ),
    _spec(
        "SST-PRS121",
        Severity.ERROR,
        "Both spellings of one key are set",
        "{artifact}: sets both '{field}' and '{expected}'",
        "delete the 0.3 spelling; sst migrate refs does not rename keys",
    ),
    _spec(
        "SST-VAL405",
        Severity.ERROR,
        "Boolean standalone filter",
        "filter '{member}' is boolean-valued and declares no labels: key",
        "add labels: [filter] so it renders as a native LABELS = (FILTER) dimension; sst migrate refs adds it",
    ),
    _spec(
        "SST-CFG036",
        Severity.ERROR,
        "Name-map entry cannot be qualified",
        "{block}: '{name}' cannot be qualified -- no fqn: and {reason}",
        "set fqn: on the entry, or set default_prefix on the block",
    ),
    _spec(
        "SST-REF032",
        Severity.ERROR,
        "skill() target not declared",
        "{{{{ skill('{name}') }}}} does not resolve",
        "declare the skill under the skills directory, or correct the name",
    ),
    _spec(
        "SST-REF036",
        Severity.ERROR,
        "plugin() target not declared",
        "{{{{ plugin('{name}') }}}} does not resolve",
        "declare the plugin under the plugins directory, or correct the name",
    ),
    _spec(
        "SST-REF037",
        Severity.ERROR,
        "extension() names an extension this project publishes",
        "agent '{artifact}': extension('{name}') names a {kind} this project publishes",
        "reference it with skill() or plugin(), which pins the published version",
    ),
    _spec(
        "SST-VAL540",
        Severity.ERROR,
        "SKILL-type extension source has no name",
        "agent '{artifact}': the skill source for skill('{path}') omits name",
        "declare name; it is optional only for a plugin",
    ),
    _spec(
        "SST-VAL804",
        Severity.WARNING,
        "Published extension is referenced by no agent",
        "{artifact}: version {value} is referenced by no agent",
        "reference it from an agent, or accept that it serves the catalog and Desktop only",
    ),
    _spec(
        "SST-VAL814",
        Severity.WARNING,
        "Bundle carries a script no consuming agent can run",
        "agent '{artifact}': {name} bundles scripts, and the agent declares no code_execution tool",
        "enable code_execution on the agent, or drop the scripts",
    ),
    _spec(
        "SST-VAL838",
        Severity.ERROR,
        "Version set on a project-published extension",
        "agent '{artifact}': skill source '{name}' sets version, which SST manages for {kind}('{path}')",
        "delete version; SST pins the published content-hash alias",
    ),
    _spec(
        "SST-VAL839",
        Severity.ERROR,
        "Skill version pinned to the commit SHA",
        "agent '{artifact}': skill source '{name}' pins var('sha_version'), which names no published version",
        "use skill() or plugin() with no version, or pin a real version of a consumed extension",
    ),
    _spec(
        "SST-VAL840",
        Severity.ERROR,
        "Skill source name does not match its extension",
        "agent '{artifact}': skill source name '{name}' must be {expected}",
        "use the skill's name, or a member name of the plugin",
    ),
    _spec(
        "SST-VAL841",
        Severity.WARNING,
        "Published version is not the extension default",
        "{artifact}: version {value} is not the default version of {target}",
        "agents pin their version; set the default in Snowflake if the catalog should show this one",
    ),
    _spec(
        "SST-PLN026",
        Severity.ERROR,
        "Stage is not an internal SSE stage",
        "{artifact}: stage {value} is {found}, expected INTERNAL NO CSE",
        "point the configuration at an internal stage with SNOWFLAKE_SSE encryption; SST never alters a stage",
        demotable=False,
    ),
    _spec(
        "SST-PLN027",
        Severity.ERROR,
        "Version alias holds different content",
        "{artifact}: version alias {value} holds files that differ from the bundle ({detail})",
        "an earlier publish left a damaged version under this alias; remove the alias in Snowflake, then plan again",
        demotable=False,
    ),
    _spec(
        "SST-VAL843",
        Severity.WARNING,
        "Two artifacts publish to one Snowflake name",
        "{a} and {b} both publish to {target}",
        "rename one of them; plans and reports that name the object become ambiguous",
    ),
    _spec(
        "SST-VAL830",
        Severity.WARNING,
        "Skill reaches neither channel",
        "skill '{artifact}' is published nowhere",
        "enable the catalog channel, or add the skill to a profile",
    ),
    _spec(
        "SST-VAL844",
        Severity.ERROR,
        "Profile names a skill the project does not have",
        "profile '{artifact}': skill '{name}' is not a skill in this project",
        "add the skill under the skills directory, or remove it from the profile",
    ),
    _spec(
        "SST-VAL845",
        Severity.WARNING,
        "Profile repeats a shared skill",
        "profile '{artifact}': skill '{name}' already reaches every profile through shared/",
        "remove it from the profile",
    ),
    _spec(
        "SST-VAL846",
        Severity.ERROR,
        "Profile names an unknown hook",
        "profile '{artifact}': hook '{name}' is not defined under the hooks directory",
        "add the hook, or remove it from the profile",
    ),
    _spec(
        "SST-VAL847",
        Severity.ERROR,
        "Profile names an unknown MCP config",
        "profile '{artifact}': MCP config '{name}' is not defined under the MCP servers directory",
        "add the config, or remove it from the profile",
    ),
    _spec(
        "SST-VAL848",
        Severity.ERROR,
        "MCP server entry is not a configuration",
        "profile '{artifact}': MCP server '{name}' is not a configuration object",
        "replace the placeholder with a server definition, or stop referencing the config",
    ),
    _spec(
        "SST-VAL849",
        Severity.ERROR,
        "Two MCP configs define one server",
        "profile '{artifact}': MCP server '{name}' is defined by both '{a}' and '{b}'",
        "rename one server, or reference only one of the configs",
    ),
    _spec(
        "SST-VAL850",
        Severity.ERROR,
        "MCP config carries a literal credential",
        "MCP config '{artifact}': server '{name}' sets '{key}' to a literal value",
        "use a ${VARIABLE} placeholder; published configs are readable by every profile user",
    ),
    _spec(
        "SST-VAL851",
        Severity.ERROR,
        "Profile key is not accepted",
        "profile '{artifact}': '{key}' is not accepted: {reason}",
        "delete the key",
    ),
    _spec(
        "SST-VAL852",
        Severity.ERROR,
        "Hook definition is incomplete or ambiguous",
        "hook '{artifact}': {detail}",
        "declare event, command, and exactly one script, naming it with script: when the folder holds several files",
    ),
    _spec(
        "SST-VAL853",
        Severity.ERROR,
        "MCP config is not valid",
        "MCP config '{artifact}': {detail}",
        'write mcp.json as {"mcpServers": {"<name>": {...}}}',
    ),
    _spec(
        "SST-VAL854",
        Severity.INFO,
        "Profile registry is not the one Desktop reads",
        "profiles publish to {value}; CoCo Desktop reads only {expected}",
        "point skills.stage at the Desktop registry to publish for real, or keep this for rehearsal",
    ),
    _spec(
        "SST-VAL855",
        Severity.ERROR,
        "Profile includes a skill with errors",
        "profile '{artifact}': skill '{name}' has errors, so the profile cannot publish",
        "fix the skill's errors",
    ),
    _spec(
        "SST-VAL856",
        Severity.ERROR,
        "Agent references an extension that cannot publish",
        "agent '{artifact}': {kind}('{name}') has no version to pin because {reason}",
        "fix the diagnostic that names the cause; the reference resolves once the extension publishes",
    ),
    _spec(
        "SST-PLN028",
        Severity.ERROR,
        "Registry row changed since SST last wrote it",
        "{artifact}: registry row '{value}' has VERSION {found}, and SST last wrote {expected}",
        "another writer changed the row; reconcile it, then plan again",
        demotable=False,
    ),
    _spec(
        "SST-PLN029",
        Severity.ERROR,
        "Registry table has the wrong shape",
        "{artifact}: {value} {detail}",
        "SST never alters the registry; fix the table or point skills.stage at a compatible one",
        demotable=False,
    ),
    _spec(
        "SST-PLN030",
        Severity.ERROR,
        "Pinned version is not part of this plan",
        "{artifact}: pins the published version of {value}; select {value} as well",
        "plan the pinned artifact in the same run; when its version is already published it plans as NOOP",
        demotable=False,
    ),
    _spec(
        "SST-APL018",
        Severity.ERROR,
        "Registry pointer write failed after an upload",
        "{artifact}: uploaded {path}, pointer write failed: {detail}",
        "re-run apply; the uploaded trees are content-addressed and are reused",
    ),
    _spec(
        "SST-APL007",
        Severity.ERROR,
        "Tag application failed after a successful create",
        "{artifact}: {detail}",
        "re-run apply; the version exists and only its certification tag is missing",
    ),
)


def _placeholders(template: str) -> frozenset[str]:
    return frozenset(field for _, field, _, _ in Formatter().parse(template) if field)


def _build_registry(specs: tuple[ErrorSpec, ...]) -> Mapping[str, ErrorSpec]:
    registry: dict[str, ErrorSpec] = {}
    for spec in specs:
        if spec.code in registry:
            raise RegistryIntegrityError(f"duplicate error code {spec.code}")
        if not spec.code.startswith("SST-") or spec.code.split("-")[1][:3] != spec.subsystem:
            raise RegistryIntegrityError(f"invalid error code {spec.code}")
        registry[spec.code] = spec
    return MappingProxyType(registry)


ERROR_REGISTRY = _build_registry((*_SPECS, *_PARITY_SPECS, *_PUBLISHING_SPECS))

LEGACY_ALIASES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "SST-V001": ("SST-PRS002", "SST-PRS107", "SST-VAL201"),
        "SST-V002": ("SST-DBT002", "SST-MEM003", "SST-REF001"),
        "SST-V003": ("SST-DBT004", "SST-REF002", "SST-VAL020"),
        "SST-V004": ("SST-PRS006", "SST-PRS106", "SST-VAL001"),
        "SST-V005": ("SST-PRS003", "SST-PRS018", "SST-PRS019", "SST-PRS029", "SST-PRS113", "SST-VAL113", "SST-VAL418"),
        "SST-V006": ("SST-PRS101", "SST-PRS107"),
        "SST-V007": ("SST-PRS013",),
        "SST-V008": ("SST-DBT004",),
        "SST-V010": ("SST-DBT003", "SST-MEM011", "SST-VAL311", "SST-VAL312"),
        "SST-V011": ("SST-VAL210", "SST-VAL310"),
        "SST-V012": ("SST-VAL003",),
        "SST-V013": ("SST-MEM011", "SST-PRS029", "SST-VAL020"),
        "SST-V014": ("SST-PRS030",),
        "SST-V015": ("SST-PRS028",),
        "SST-V016": ("SST-PRS027",),
        "SST-V020": ("SST-VAL003",),
        "SST-V021": ("SST-VAL308",),
        "SST-V022": ("SST-VAL309",),
        "SST-V023": ("SST-VAL305",),
        "SST-V024": ("SST-VAL306",),
        "SST-V025": ("SST-MEM103", "SST-VAL314", "SST-VAL315"),
        "SST-V032": ("SST-MEM002", "SST-VAL109"),
        "SST-V033": ("SST-PRS002", "SST-PRS113"),
        "SST-V034": ("SST-PRS102",),
        "SST-V035": ("SST-PRS013", "SST-VAL121", "SST-VAL122"),
        "SST-V036": ("SST-VAL118",),
        "SST-V037": ("SST-PRS003", "SST-VAL102"),
        "SST-V038": ("SST-PRS003", "SST-VAL115"),
        "SST-V039": ("SST-VAL112",),
        "SST-V040": ("SST-PRS002", "SST-VAL201"),
        "SST-V041": ("SST-REF001", "SST-VAL205"),
        "SST-V042": ("SST-VAL203",),
        "SST-V043": ("SST-REF002", "SST-VAL204"),
        "SST-V044": ("SST-VAL204", "SST-VAL214"),
        "SST-V045": ("SST-REF006",),
        "SST-V046": ("SST-VAL108",),
        "SST-V047": ("SST-VAL318",),
        "SST-V048": ("SST-VAL223",),
        "SST-V049": ("SST-PRS110", "SST-VAL213"),
        "SST-V050": ("SST-VAL403",),
        "SST-V051": ("SST-VAL401",),
        "SST-V052": ("SST-PRS020", "SST-VAL012"),
        "SST-V060": ("SST-PRS002", "SST-VAL020", "SST-VAL412"),
        "SST-V061": ("SST-LOD018", "SST-LOD019"),
        "SST-V062": ("SST-PRS014", "SST-VAL412"),
        "SST-V070": ("SST-PRS002", "SST-VAL003"),
        "SST-V071": ("SST-MEM008", "SST-REF001"),
        "SST-V081": (),
        "SST-V090": ("SST-PLN005", "SST-REF005", "SST-VAL010", "SST-VAL215"),
        "SST-V091": ("SST-PRS006", "SST-VAL124"),
        "SST-V092": ("SST-VAL110",),
    }
)


def resolve_code(code: str) -> tuple[str, ...]:
    if code in ERROR_REGISTRY:
        return (code,)
    return LEGACY_ALIASES.get(code, ())


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


def dedupe_diagnostics(diagnostics: DiagnosticBag) -> DiagnosticBag:
    """Keep one deterministic copy of every diagnostic identity."""
    seen: set[str] = set()
    unique: list[Diagnostic] = []
    for diagnostic in diagnostics:
        if diagnostic.fingerprint in seen:
            continue
        seen.add(diagnostic.fingerprint)
        unique.append(diagnostic)
    return DiagnosticBag(unique)


def render_diagnostic(diagnostic: Diagnostic) -> str:
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
