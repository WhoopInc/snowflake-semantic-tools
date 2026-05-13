"""
Error Code Registry

Stable, searchable error codes for all SST diagnostics. Each code is a permanent
identifier that agents and documentation can reference.

Code format: SST-{CATEGORY}{NUMBER:03d}
  - SST-V0xx: Validation errors (tables, columns, metrics, relationships, etc.)
  - SST-P0xx: Parsing errors (YAML syntax, template resolution)
  - SST-E0xx: Extract errors (connection, schema, permission, data loading)
  - SST-G0xx: Generate errors (DDL execution, name collision, missing metadata)
  - SST-C0xx: Config errors (missing files, invalid settings)
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class ErrorCategory(Enum):
    VALIDATION = "V"
    PARSING = "P"
    EXTRACT = "E"
    GENERATE = "G"
    CONFIG = "C"


@dataclass(frozen=True)
class ErrorSpec:
    code: str
    title: str
    category: ErrorCategory
    suggestion_template: str
    doc_anchor: str
    deprecated_by: Optional[str] = None

    @property
    def url(self) -> str:
        return f"https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/reference/error-codes.md{self.doc_anchor}"


ERRORS: Dict[str, ErrorSpec] = {}


def _register(code: str, title: str, category: ErrorCategory, suggestion: str = "", anchor: str = "") -> ErrorSpec:
    spec = ErrorSpec(
        code=code,
        title=title,
        category=category,
        suggestion_template=suggestion,
        doc_anchor=anchor or f"#{code.lower().replace('-', '')}",
    )
    ERRORS[code] = spec
    return spec


# ===========================================================================
# VALIDATION ERRORS (SST-V0xx)
# ===========================================================================

# --- Required fields ---
_register(
    "SST-V001", "Missing required field", ErrorCategory.VALIDATION, "Add '{field}' to the {entity_type} definition"
)
_register(
    "SST-V002",
    "Unknown table reference",
    ErrorCategory.VALIDATION,
    "Did you mean '{closest_match}'? Available: {available}",
)
_register(
    "SST-V003",
    "Unknown column reference",
    ErrorCategory.VALIDATION,
    "Column '{column}' not found in table '{table}'. Available: {available}",
)
_register("SST-V004", "Duplicate name", ErrorCategory.VALIDATION, "'{name}' is also defined at {other_location}")
_register(
    "SST-V005", "Invalid field type", ErrorCategory.VALIDATION, "'{field}' must be {expected_type}, got {actual_type}"
)
_register("SST-V006", "Empty required field", ErrorCategory.VALIDATION, "'{field}' cannot be empty")
_register(
    "SST-V007", "Invalid column_type", ErrorCategory.VALIDATION, "Must be one of: dimension, fact, time_dimension"
)
_register(
    "SST-V008",
    "Invalid data_type",
    ErrorCategory.VALIDATION,
    "Valid types: TEXT, NUMBER, FLOAT, BOOLEAN, DATE, TIMESTAMP_NTZ, TIMESTAMP_LTZ, VARIANT, ARRAY, OBJECT. Or run: DESCRIBE TABLE <table> to see actual types",
)

# --- Table validation ---
_register(
    "SST-V010",
    "Missing primary_key",
    ErrorCategory.VALIDATION,
    "Add primary_key to config.meta.sst:\n  primary_key: {suggested_column}",
)
_register(
    "SST-V011",
    "Primary key column not found",
    ErrorCategory.VALIDATION,
    "Column '{column}' listed in primary_key does not exist in the model",
)
_register(
    "SST-V012", "Missing table description", ErrorCategory.VALIDATION, "Add a description to the model definition"
)
_register("SST-V013", "Invalid synonyms type", ErrorCategory.VALIDATION, "'synonyms' must be a list, not {actual_type}")
_register(
    "SST-V014",
    "Problematic characters in synonyms",
    ErrorCategory.VALIDATION,
    "Remove apostrophes/quotes from synonyms, or run: sst format --sanitize",
)
_register(
    "SST-V015",
    "Invalid constraints configuration",
    ErrorCategory.VALIDATION,
    "Constraint '{name}' is missing required field: {field}",
)
_register(
    "SST-V016",
    "Invalid tags configuration",
    ErrorCategory.VALIDATION,
    "Tags must be a dict with fully-qualified names (DB.SCHEMA.TAG_NAME)",
)

# --- Column validation ---
_register("SST-V020", "Missing column description", ErrorCategory.VALIDATION, "Add a description to column '{column}'")
_register(
    "SST-V021",
    "Missing column_type metadata",
    ErrorCategory.VALIDATION,
    "Add column_type to config.meta.sst: column_type: dimension|fact|time_dimension",
)
_register(
    "SST-V022",
    "Missing data_type metadata",
    ErrorCategory.VALIDATION,
    "Add data_type to config.meta.sst: data_type: TEXT|NUMBER|TIMESTAMP_NTZ|...",
)
_register(
    "SST-V023",
    "Fact column must be numeric",
    ErrorCategory.VALIDATION,
    "Column '{column}' is type 'fact' but has non-numeric data_type '{data_type}'",
)
_register(
    "SST-V024",
    "Time dimension must be temporal",
    ErrorCategory.VALIDATION,
    "Column '{column}' is type 'time_dimension' but has non-temporal data_type '{data_type}'",
)
_register(
    "SST-V025",
    "Enum without sample_values",
    ErrorCategory.VALIDATION,
    "is_enum=true requires sample_values list with allowed values",
)

# --- Metric validation ---
_register(
    "SST-V032", "Missing metric tables", ErrorCategory.VALIDATION, "Add 'tables' list with at least one table reference"
)
_register(
    "SST-V033",
    "Invalid metric expression type",
    ErrorCategory.VALIDATION,
    "'expr' must be a string containing a SQL expression",
)
_register(
    "SST-V034",
    "Empty metric tables list",
    ErrorCategory.VALIDATION,
    "'tables' must contain at least one table reference",
)
_register(
    "SST-V035", "Invalid visibility value", ErrorCategory.VALIDATION, "'visibility' must be 'private' or 'public'"
)
_register(
    "SST-V036",
    "Invalid non_additive_by configuration",
    ErrorCategory.VALIDATION,
    "non_additive_by entries must have a 'dimension' field",
)
_register(
    "SST-V037",
    "Invalid window metric configuration",
    ErrorCategory.VALIDATION,
    "Window metrics need 'window_function' and 'order_by' fields",
)
_register(
    "SST-V038",
    "Invalid using_relationships type",
    ErrorCategory.VALIDATION,
    "'using_relationships' must be a list of relationship names",
)
_register(
    "SST-V039",
    "Cross-entity column reference in metric expression",
    ErrorCategory.VALIDATION,
    "Split into separate single-table metrics, or use metric composition via {{ metric() }}",
)
_register(
    "SST-V045",
    "Derived metric must reference other metrics",
    ErrorCategory.VALIDATION,
    "Use: expr: \"{{ metric('metric_a') }} + {{ metric('metric_b') }}\"",
)
_register(
    "SST-V046",
    "Invalid field on derived metric",
    ErrorCategory.VALIDATION,
    "Remove the invalid fields — derived metrics are view-scoped",
)
_register(
    "SST-V047",
    "Excluded column referenced in expression",
    ErrorCategory.VALIDATION,
    "Remove the reference or un-exclude the column (config.meta.sst.exclude: false)",
)
_register(
    "SST-V048",
    "Primary key and unique keys overlap",
    ErrorCategory.VALIDATION,
    "Remove overlapping columns from unique_keys — primary_key is already unique",
)
_register(
    "SST-V049",
    "Unsupported multi-column expression in relationship condition",
    ErrorCategory.VALIDATION,
    "Expressions in relationship conditions must reference exactly one column. "
    "Multi-column expressions (e.g., COALESCE(col1, col2)) are not supported for join key generation.",
)
_register(
    "SST-V040",
    "Missing relationship field",
    ErrorCategory.VALIDATION,
    "Relationship '{name}' is missing required field: {field}",
)
_register(
    "SST-V041",
    "Relationship table not found",
    ErrorCategory.VALIDATION,
    "Table '{table}' in relationship '{name}' not found. Did you mean '{closest_match}'?",
)
_register(
    "SST-V042",
    "Relationship missing primary key",
    ErrorCategory.VALIDATION,
    "Right table '{table}' needs a primary_key for join. Add primary_key to config.meta.sst",
)
_register(
    "SST-V043",
    "Relationship column not found",
    ErrorCategory.VALIDATION,
    "Column '{column}' not found in table '{table}'",
)
_register(
    "SST-V044",
    "Using relationship not found",
    ErrorCategory.VALIDATION,
    "Relationship '{name}' referenced in using_relationships not found. Available: {available}",
)

# --- Filter validation ---
_register(
    "SST-V050",
    "Deprecated filters syntax",
    ErrorCategory.VALIDATION,
    "Migrate to snowflake_custom_instructions with ai_sql_generation text",
)
_register(
    "SST-V051",
    "Invalid filter expression",
    ErrorCategory.VALIDATION,
    "Filter expression must be a non-empty SQL string",
)
_register(
    "SST-V052",
    "Deprecated custom instruction key names",
    ErrorCategory.VALIDATION,
    "Use 'ai_sql_generation' and 'ai_question_categorization' to align with Snowflake DDL",
)

# --- Verified query validation ---
_register(
    "SST-V060",
    "Missing verified query field",
    ErrorCategory.VALIDATION,
    "Verified query '{name}' is missing required field: {field}",
)
_register(
    "SST-V061",
    "VQR sql_file not found",
    ErrorCategory.VALIDATION,
    "File '{path}' does not exist. Check the path relative to the YAML file",
)
_register(
    "SST-V062",
    "VQR mutual exclusivity violation",
    ErrorCategory.VALIDATION,
    "Specify either 'sql' or 'sql_file', not both",
)

# --- Semantic view validation ---
_register(
    "SST-V070",
    "Missing semantic view field",
    ErrorCategory.VALIDATION,
    "Semantic view '{name}' is missing required field: {field}",
)
_register(
    "SST-V071",
    "Semantic view references unknown table",
    ErrorCategory.VALIDATION,
    "Table '{table}' not found. Did you mean '{closest_match}'?",
)

# --- Template validation ---
_register(
    "SST-V081",
    "Quoted template expression",
    ErrorCategory.VALIDATION,
    "Remove quotes around template: use {{ ref('name') }} not '{{ ref('name') }}'",
)

# --- Dependency validation ---
_register("SST-V090", "Circular dependency", ErrorCategory.VALIDATION, "Cycle detected: {cycle}")
_register(
    "SST-V091",
    "Duplicate metric expressions",
    ErrorCategory.VALIDATION,
    "Metrics '{name1}' and '{name2}' have identical expressions",
)
_register(
    "SST-V092",
    "Metric references undeclared column",
    ErrorCategory.VALIDATION,
    "Column '{table}.{column}' in metric '{metric}' is not declared as a fact or dimension. "
    "Add column_type: fact (or dimension) to the column in the YAML for table '{table}'",
)

# ===========================================================================
# PARSING ERRORS (SST-P0xx)
# ===========================================================================

_register(
    "SST-P001", "YAML syntax error", ErrorCategory.PARSING, "Check indentation and special characters at line {line}"
)
_register(
    "SST-P002",
    "YAML colon in description",
    ErrorCategory.PARSING,
    "Quote the string or use multiline syntax: description: |-",
)
_register(
    "SST-P003",
    "Template parsing error",
    ErrorCategory.PARSING,
    "Template syntax is invalid. Use: {{{{ ref('table_name') }}}}",
)
_register("SST-P004", "Invalid YAML structure", ErrorCategory.PARSING, "Expected {expected} but found {actual}")

# ===========================================================================
# EXTRACT ERRORS (SST-E0xx)
# ===========================================================================

_register(
    "SST-E001",
    "Snowflake connection failed",
    ErrorCategory.EXTRACT,
    "Check credentials in ~/.dbt/profiles.yml. Run: sst debug --test-connection",
)
_register(
    "SST-E002",
    "Schema not found",
    ErrorCategory.EXTRACT,
    "Schema '{schema}' does not exist in database '{database}'. Create it or check spelling",
)
_register(
    "SST-E003",
    "Permission denied",
    ErrorCategory.EXTRACT,
    "Role needs CREATE TABLE on schema. Grant: GRANT CREATE TABLE ON SCHEMA {schema} TO ROLE {role}",
)
_register("SST-E004", "Manifest not found", ErrorCategory.EXTRACT, "Run 'dbt compile' first, or use --dbt-compile flag")
_register(
    "SST-E005", "Stale manifest", ErrorCategory.EXTRACT, "manifest.json is older than source files. Re-run: dbt compile"
)

# --- Source enrichment errors ---
_register(
    "SST-E006",
    "Source not found in manifest",
    ErrorCategory.EXTRACT,
    "Source '{source_name}.{table_name}' not in manifest. Run 'dbt compile' or check source name",
)
_register(
    "SST-E007",
    "Source table not found in Snowflake",
    ErrorCategory.EXTRACT,
    "Table '{database}.{schema}.{table}' does not exist. Check database/schema in your source definition",
)
_register(
    "SST-E008",
    "No sources found",
    ErrorCategory.EXTRACT,
    "No dbt source definitions found. Ensure sources.yml exists with a 'sources:' key",
)
_register(
    "SST-E009",
    "Source YAML write failed",
    ErrorCategory.EXTRACT,
    "Could not write enriched metadata to '{path}'. Check file permissions",
)
_register(
    "SST-E010",
    "Invalid source selector",
    ErrorCategory.EXTRACT,
    "Source selector must be format 'source_name.table_name'. Got: '{value}'",
)

# ===========================================================================
# GENERATE ERRORS (SST-G0xx)
# ===========================================================================

_register(
    "SST-G001",
    "DDL execution failed",
    ErrorCategory.GENERATE,
    "Snowflake rejected the CREATE SEMANTIC VIEW statement. See SQL and error above",
)
_register(
    "SST-G002",
    "Object name collision",
    ErrorCategory.GENERATE,
    "Object '{name}' already exists as {object_type}. Rename view or drop: DROP {object_type} {name}",
)
_register(
    "SST-G003",
    "Missing metadata tables",
    ErrorCategory.GENERATE,
    "SM_* tables not found. Run 'sst extract' first to populate metadata",
)
_register(
    "SST-G004",
    "Table not found in Snowflake",
    ErrorCategory.GENERATE,
    "Table '{table}' does not exist. Check database/schema or use --defer-target",
)
_register(
    "SST-G005",
    "Insufficient privileges",
    ErrorCategory.GENERATE,
    "Role needs CREATE SEMANTIC VIEW on schema. Grant: GRANT CREATE SEMANTIC VIEW ON SCHEMA {schema} TO ROLE {role}",
)
_register(
    "SST-G006",
    "SQL output write failed",
    ErrorCategory.GENERATE,
    "Could not write generated SQL to '{path}'. Check directory permissions and disk space",
)

# ===========================================================================
# CONFIG ERRORS (SST-C0xx)
# ===========================================================================

_register(
    "SST-C001",
    "Missing sst_config.yml",
    ErrorCategory.CONFIG,
    "Run 'sst init' to create configuration, or create sst_config.yml manually",
)
_register(
    "SST-C002",
    "Missing dbt_project.yml",
    ErrorCategory.CONFIG,
    "SST must be run from a dbt project directory containing dbt_project.yml",
)
_register(
    "SST-C003",
    "Invalid configuration",
    ErrorCategory.CONFIG,
    "Config key '{key}' has invalid value. Expected: {expected}",
)
_register(
    "SST-C004",
    "Missing profiles.yml",
    ErrorCategory.CONFIG,
    "Create ~/.dbt/profiles.yml with Snowflake connection details",
)


def get_error(code: str) -> ErrorSpec:
    if code not in ERRORS:
        raise KeyError(f"Unknown error code: {code}")
    return ERRORS[code]


def get_errors_by_category(category: ErrorCategory) -> Dict[str, ErrorSpec]:
    return {k: v for k, v in ERRORS.items() if v.category == category}
