# Error Code Reference

Stable error codes for SST diagnostics. Each code is a permanent identifier that agents and documentation can reference.

---

## Validation Errors (SST-Vxxx)

| Code | Title | Suggestion |
|------|-------|------------|
| `SST-V001` | Missing required field | Add '{field}' to the {entity_type} definition |
| `SST-V002` | Unknown table reference | Did you mean '{closest_match}'? Available: {available} |
| `SST-V003` | Unknown column reference | Column '{column}' not found in table '{table}'. Available: {available} |
| `SST-V004` | Duplicate name | '{name}' is also defined at {other_location} |
| `SST-V005` | Invalid field type | '{field}' must be {expected_type}, got {actual_type} |
| `SST-V006` | Empty required field | '{field}' cannot be empty |
| `SST-V007` | Invalid column_type | Must be one of: dimension, fact, time_dimension |
| `SST-V008` | Invalid data_type | '{value}' is not a valid Snowflake data type |
| `SST-V010` | Missing primary_key | Add primary_key to config.meta.sst: |
| `SST-V011` | Primary key column not found | Column '{column}' listed in primary_key does not exist in the model |
| `SST-V012` | Missing table description | Add a description to the model definition |
| `SST-V013` | Invalid synonyms type | 'synonyms' must be a list, not {actual_type} |
| `SST-V014` | Problematic characters in synonyms | Remove apostrophes/quotes from synonyms, or run: sst format --sanitize |
| `SST-V015` | Invalid constraints configuration | Constraint '{name}' is missing required field: {field} |
| `SST-V016` | Invalid tags configuration | Tags must be a dict with fully-qualified names (DB.SCHEMA.TAG_NAME) |
| `SST-V020` | Missing column description | Add a description to column '{column}' |
| `SST-V021` | Missing column_type metadata | Add column_type to config.meta.sst: column_type: dimension|fact|time_dimension |
| `SST-V022` | Missing data_type metadata | Add data_type to config.meta.sst: data_type: TEXT|NUMBER|TIMESTAMP_NTZ|... |
| `SST-V023` | Fact column must be numeric | Column '{column}' is type 'fact' but has non-numeric data_type '{data_type}' |
| `SST-V024` | Time dimension must be temporal | Column '{column}' is type 'time_dimension' but has non-temporal data_type '{data_type}' |
| `SST-V025` | Enum without sample_values | is_enum=true requires sample_values list with allowed values |
| `SST-V032` | Missing metric tables | Add 'tables' list with at least one table reference |
| `SST-V033` | Invalid metric expression type | 'expr' must be a string containing a SQL expression |
| `SST-V034` | Empty metric tables list | 'tables' must contain at least one table reference |
| `SST-V035` | Invalid visibility value | 'visibility' must be 'private' or 'public' |
| `SST-V036` | Invalid non_additive_by configuration | non_additive_by entries must have a 'dimension' field |
| `SST-V037` | Invalid window metric configuration | Window metrics need 'window_function' and 'order_by' fields |
| `SST-V038` | Invalid using_relationships type | 'using_relationships' must be a list of relationship names |
| `SST-V039` | Cross-entity column reference | Metric expression references columns from multiple entities. Split into single-table metrics or use derived: true |
| `SST-V040` | Missing relationship field | Relationship '{name}' is missing required field: {field} |
| `SST-V041` | Relationship table not found | Table '{table}' in relationship '{name}' not found. Did you mean '{closest_match}'? |
| `SST-V042` | Relationship missing primary key | Right table '{table}' needs a primary_key for join. Add primary_key to config.meta.sst |
| `SST-V043` | Relationship column not found | Column '{column}' not found in table '{table}'. Note: single-column [expression-based joins](../concepts/semantic-models.md#expression-based-joins-auto-generated-join-key-dimensions) are supported and auto-generate join key dimensions |
| `SST-V044` | Using relationship not found | Relationship '{name}' referenced in using_relationships not found. Available: {available} |
| `SST-V045` | Derived metric must use metric references | Derived metrics must use {{ metric('name') }} syntax, not raw column expressions |
| `SST-V046` | Invalid field on derived metric | Derived metrics cannot use using_relationships, non_additive_by, or window |
| `SST-V047` | Excluded column conflict | Column has both exclude: true and column_type set. Excluded columns are omitted from semantic views |
| `SST-V048` | Primary key and unique keys overlap | Columns in both primary_key and unique_keys. Remove duplicates from unique_keys |
| `SST-V049` | Multi-column expression in relationship | Expressions in join conditions must reference exactly one column. Use a single-column expression, or add a computed column to the dbt model |
| `SST-V050` | Deprecated filters syntax | Migrate to snowflake_custom_instructions with ai_sql_generation text |
| `SST-V051` | Invalid filter expression | Filter expression must be a non-empty SQL string |
| `SST-V052` | Deprecated custom instruction key names | Use 'ai_sql_generation' and 'ai_question_categorization' to align with Snowflake DDL |
| `SST-V060` | Missing verified query field | Verified query '{name}' is missing required field: {field} |
| `SST-V061` | VQR sql_file not found | File '{path}' does not exist. Check the path relative to the YAML file |
| `SST-V062` | VQR mutual exclusivity violation | Specify either 'sql' or 'sql_file', not both |
| `SST-V070` | Missing semantic view field | Semantic view '{name}' is missing required field: {field} |
| `SST-V071` | Semantic view references unknown table | Table '{table}' not found. Did you mean '{closest_match}'? |
| `SST-V081` | Quoted template expression | Remove quotes around template: use {{ ref('name') }} not '{{ ref('name') }}' |
| `SST-V090` | Circular dependency | Cycle detected: {cycle} |
| `SST-V091` | Duplicate metric expressions | Metrics '{name1}' and '{name2}' have identical expressions |

## Parsing Errors (SST-Pxxx)

| Code | Title | Suggestion |
|------|-------|------------|
| `SST-P001` | YAML syntax error | Check indentation and special characters at line {line} |
| `SST-P002` | YAML colon in description | Quote the string or use multiline syntax: description: |- |
| `SST-P003` | Template parsing error | Template syntax is invalid. Use: {{{{ ref('table_name') }}}} |
| `SST-P004` | Invalid YAML structure | Expected {expected} but found {actual} |

## Extract Errors (SST-Exxx)

| Code | Title | Suggestion |
|------|-------|------------|
| `SST-E001` | Snowflake connection failed | Check credentials in ~/.dbt/profiles.yml. Run: sst debug --test-connection |
| `SST-E002` | Schema not found | Schema '{schema}' does not exist in database '{database}'. Create it or check spelling |
| `SST-E003` | Permission denied | Role needs CREATE TABLE on schema. Grant: GRANT CREATE TABLE ON SCHEMA {schema} TO ROLE {role} |
| `SST-E004` | Manifest not found | Run 'dbt compile' first, or use --dbt-compile flag |
| `SST-E005` | Stale manifest | manifest.json is older than source files. Re-run: dbt compile |
| `SST-E006` | Source not found in manifest | Source '{source_name}.{table_name}' not in manifest. Run `dbt compile` or check source name |
| `SST-E007` | Source table not found in Snowflake | Table '{database}.{schema}.{table}' does not exist. Check database/schema in your source definition |
| `SST-E008` | No sources found | No dbt source definitions found. Ensure sources.yml exists with a `sources:` key |
| `SST-E009` | Source YAML write failed | Could not write enriched metadata to '{path}'. Check file permissions |
| `SST-E010` | Invalid source selector | Source selector must be format 'source_name.table_name'. Got: '{value}' |

## Generate Errors (SST-Gxxx)

| Code | Title | Suggestion |
|------|-------|------------|
| `SST-G001` | DDL execution failed | Snowflake rejected the CREATE SEMANTIC VIEW statement. See SQL and error above |
| `SST-G002` | Object name collision | Object '{name}' already exists as {object_type}. Rename view or drop: DROP {object_type} {name} |
| `SST-G003` | Missing metadata tables | SM_* tables not found. Run 'sst extract' first to populate metadata |
| `SST-G004` | Table not found in Snowflake | Table '{table}' does not exist. Check database/schema or use --defer-target |
| `SST-G005` | Insufficient privileges | Role needs CREATE SEMANTIC VIEW on schema. Grant: GRANT CREATE SEMANTIC VIEW ON SCHEMA {schema} TO ROLE {role} |

## Config Errors (SST-Cxxx)

| Code | Title | Suggestion |
|------|-------|------------|
| `SST-C001` | Missing sst_config.yml | Run 'sst init' to create configuration, or create sst_config.yml manually |
| `SST-C002` | Missing dbt_project.yml | SST must be run from a dbt project directory containing dbt_project.yml |
| `SST-C003` | Invalid configuration | Config key '{key}' has invalid value. Expected: {expected} |
| `SST-C004` | Missing profiles.yml | Create ~/.dbt/profiles.yml with Snowflake connection details |

---

## Adding New Error Codes

To add a new error code:

1. Add to `snowflake_semantic_tools/core/diagnostics/error_codes.py`:
   ```python
   _register("SST-VXXX", "Title", ErrorCategory.VALIDATION, "Suggestion template")
   ```
2. Use in validation rule: `rule_id="SST-VXXX", suggestion="Specific fix"`
3. CI guard tests will fail if any `add_error()` call is missing `rule_id=` or `suggestion=`
4. Coverage tests will fail if a registered code is never used
