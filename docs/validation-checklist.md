# SST Validation Checklist

Complete list of all validation checks performed by `sst validate`.

---

| Component | Check | Severity |
|-----------|-------|----------|
| **Prerequisites** | `sst_config.yml` exists in project root | REQUIRED|
| **Prerequisites** | `manifest.json` exists (run `dbt compile` or use `--dbt-compile`) | REQUIRED|
| **Prerequisites** | Manifest is fresh (< 24 hours old, no .sql files modified since) | WARNING|
| **Table** | Model has `description` | ERROR|
| **Table** | Model has `primary_key` in `meta.sst` | ERROR|
| **Table** | `primary_key` is not empty list | ERROR|
| **Table** | `primary_key` is a list (not dict, not other type) | ERROR|
| **Table** | Table name matches model name | ERROR|
| **Table** | Primary key column(s) exist in the model's column list | ERROR|
| **Table** | `synonyms` is a list if present (not string or other type) | ERROR|
| **Table** | Table has `synonyms` defined | WARNING|
| **Table** | Table synonyms don't contain apostrophes or special characters | WARNING|
| **Column** | Column has `description` | ERROR|
| **Column** | Column has `column_type` in `meta.sst` | ERROR|
| **Column** | `column_type` is valid (dimension, fact, or time_dimension) | ERROR|
| **Column** | Column has `data_type` in `meta.sst` | ERROR|
| **Column** | `data_type` is valid Snowflake type | ERROR|
| **Column** | Fact columns have numeric data types | ERROR|
| **Column** | Time dimension columns have temporal data types | ERROR|
| **Column** | `synonyms` is a list if present (not string or other type) | ERROR|
| **Column** | `sample_values` is a list if present (not string or other type) | ERROR|
| **Column** | Direct identifier PII columns have NO `sample_values` | ERROR|
| **Column** | Sample values don't contain Jinja characters (`{{`, `}}`, `{%`, etc.) | ERROR|
| **Column** | `is_enum=false` for fact columns | ERROR|
| **Column** | `is_enum=false` for time_dimension columns | ERROR|
| **Column** | `is_enum=true` columns have `sample_values` | WARNING|
| **Column** | Column synonyms don't contain apostrophes or special characters | WARNING|
| **Column** | Dimension columns have `sample_values` | INFO|
| **Column** | Non-common columns have `synonyms` | INFO|
| **Metric** | Metric has `name` | ERROR|
| **Metric** | Metric has `expr` | ERROR|
| **Metric** | Metric has `tables` | ERROR|
| **Metric** | `name` is not empty string | ERROR|
| **Metric** | `expr` is a string (not int, list, etc.) | ERROR|
| **Metric** | `expr` is not empty or whitespace-only | ERROR|
| **Metric** | `tables` is a list (not string) | ERROR|
| **Metric** | `tables` is not empty list | ERROR|
| **Metric** | `synonyms` is a list if present (not string) | ERROR|
| **Metric** | All referenced tables exist in dbt catalog | ERROR|
| **Metric** | All referenced columns exist in their tables | ERROR|
| **Metric** | No duplicate metric names | ERROR|
| **Metric** | Metric has `description` | WARNING|
| **Metric** | `default_aggregation` is valid if present | WARNING|
| **Relationship** | Relationship has `name` | ERROR|
| **Relationship** | Relationship has `left_table` | ERROR|
| **Relationship** | Relationship has `right_table` | ERROR|
| **Relationship** | Relationship has `relationship_conditions` | ERROR|
| **Relationship** | `relationship_conditions` is a list (not string or dict) | ERROR|
| **Relationship** | `relationship_conditions` is not empty | ERROR|
| **Relationship** | Each condition is a string | ERROR|
| **Relationship** | Each condition has valid operator (=, >=, <=, BETWEEN) | ERROR|
| **Relationship** | Each condition references valid tables | ERROR|
| **Relationship** | Conditions with templates must be quoted (YAML requirement) | PARSE ERROR|
| **Relationship** | `left_table` exists in dbt catalog | ERROR|
| **Relationship** | `right_table` exists in dbt catalog | ERROR|
| **Relationship** | Left join columns exist in left table | ERROR|
| **Relationship** | Right join columns exist in right table | ERROR|
| **Relationship** | Right join columns reference primary key or unique columns | ERROR|
| **Relationship** | Composite primary keys are fully referenced (not partial) | ERROR|
| **Relationship** | No SQL transformations in column references (no `::`, `CAST`, functions) | ERROR|
| **Relationship** | No duplicate relationship names | ERROR|
| **Relationship** | No circular dependencies in relationship chains | ERROR|
| **Filter** | Filter has `name` | ERROR|
| **Filter** | Filter has `expr` | ERROR|
| **Filter** | `name` is not empty string | ERROR|
| **Filter** | `expr` is a string (not int, list, etc.) | ERROR|
| **Filter** | `expr` is not empty or whitespace-only | ERROR|
| **Filter** | All referenced tables exist in dbt catalog | ERROR|
| **Filter** | All referenced columns exist in their tables | ERROR|
| **Filter** | No duplicate filter names | ERROR|
| **Filter** | Filter has `description` | WARNING|
| **Custom Instruction** | Instruction has `name` | ERROR|
| **Custom Instruction** | Instruction has `instruction` field | ERROR|
| **Custom Instruction** | `name` is not empty string | ERROR|
| **Custom Instruction** | `instruction` is not empty or whitespace-only | ERROR|
| **Custom Instruction** | No duplicate instruction names | ERROR|
| **Custom Instruction** | `instruction` text is at least 10 characters | WARNING|
| **Verified Query** | Query has `name` | ERROR|
| **Verified Query** | Query has `question` | ERROR|
| **Verified Query** | Query has `sql` | ERROR|
| **Verified Query** | `question` is not empty or whitespace-only | ERROR|
| **Verified Query** | `use_as_onboarding` is boolean if present (not string/int) | ERROR|
| **Verified Query** | `verified_at` is in YYYY-MM-DD format if present | ERROR|
| **Verified Query** | All referenced tables exist in dbt catalog | ERROR|
| **Verified Query** | No duplicate query names | ERROR|
| **Verified Query** | SQL starts with SELECT or WITH | WARNING|
| **Verified Query** | SQL table references match declared `tables` list | WARNING|
| **Semantic View** | View has `name` | ERROR|
| **Semantic View** | View has `tables` | ERROR|
| **Semantic View** | `name` is not empty string | ERROR|
| **Semantic View** | `tables` is a list (not string) OR valid JSON string | ERROR|
| **Semantic View** | `tables` is not empty | ERROR|
| **Semantic View** | All referenced tables exist in dbt catalog | ERROR|
| **Semantic View** | Referenced metrics exist | ERROR|
| **Semantic View** | Referenced relationships exist | ERROR|
| **Semantic View** | Referenced custom instructions exist | ERROR|
| **Semantic View** | No duplicate view names | ERROR|
| **Semantic View** | View has `description` | WARNING|
| **Semantic View** | No identical table lists across multiple views | WARNING|
| **Template** | All `{{ table('name') }}` templates resolve to existing tables | ERROR|
| **Template** | All `{{ column('table', 'col') }}` templates resolve to existing columns | ERROR|
| **Template** | All `{{ metric('name') }}` templates resolve to existing metrics | ERROR|
| **Template** | All `{{ custom_instructions('name') }}` templates resolve to existing instructions | ERROR|
| **Template** | No circular dependencies in metric references | ERROR|
| **Template** | No malformed template syntax | ERROR|
| **References** | Unknown table references include "Did you mean?" suggestions | ERROR|
| **Schema** | YAML columns exist in Snowflake tables (with `--verify-schema`) | ERROR|
| **Schema** | YAML columns include "Did you mean?" suggestions for typos | ERROR|

---

## Severity Levels

| Severity | Meaning | Blocks Validation? |
|----------|---------|-------------------|
| REQUIRED | Must exist before validation runs | Yes (exit code 1) |
| ERROR | Critical issue that prevents generation | Yes (exit code 1) |
| WARNING | Issue that should be reviewed | Only if `--strict` mode |
| INFO | Suggestion for improvement | No |

---

## Running Validation

```bash
# Standard validation (offline - no Snowflake connection)
sst validate

# Auto-compile manifest if missing/stale
sst validate --dbt-compile

# Verify columns exist in Snowflake (requires connection)
sst validate --verify-schema

# Strict mode (warnings block deployment)
sst validate --strict

# Verbose output
sst validate --verbose

# Examples with other flags
sst validate --exclude _intermediate,staging
```

---

## Key Notes

- **Database/Schema**: Auto-detected from `manifest.json`. Do NOT specify in `meta.sst`.
- **Required in `meta.sst`**: `primary_key` (table level), `column_type` and `data_type` (column level)
- **Optional in `meta.sst`**: `unique_keys` (table level) - required only for ASOF relationships
- **Schema Verification**: Use `--verify-schema` to validate YAML columns against Snowflake (requires credentials)
- **Fuzzy Matching**: Error messages include "Did you mean?" suggestions for typos

---

**Last Updated**: January 4, 2026  
**SST Version**: 0.1.2

