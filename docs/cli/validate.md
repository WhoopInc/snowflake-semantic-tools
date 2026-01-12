# sst validate

Validate semantic models against dbt definitions.

---

## Overview

The `validate` command checks your semantic models for errors before deployment. It performs 99+ validation checks covering table references, column references, template resolution, circular dependencies, and optionally validates SQL syntax against Snowflake.

**Snowflake Connection:** Not required (optional for `--verify-schema` and `--snowflake-syntax-check`)

---

## Quick Start

```bash
# Basic validation (no Snowflake connection needed)
sst validate

# Validate with SQL syntax checking (requires Snowflake)
sst validate --snowflake-syntax-check

# Strict mode (warnings block deployment)
sst validate --strict
```

---

## Syntax

```bash
sst validate [OPTIONS]
```

---

## Options

### Validation Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--dbt` | | PATH | Auto-detect | Path to dbt models directory |
| `--semantic` | | PATH | Auto-detect | Path to semantic models directory |
| `--strict` | | FLAG | False | Fail on warnings (not just errors) |
| `--exclude` | | TEXT | | Comma-separated directories to exclude |
| `--verbose` | `-v` | FLAG | False | Show detailed validation output |

### dbt Integration

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--dbt-compile` | | FLAG | False | Auto-run `dbt compile` before validation |

### Snowflake Verification (Optional)

These options require a Snowflake connection:

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--verify-schema` | | FLAG | False | Verify YAML columns exist in Snowflake tables |
| `--target` | `-t` | TEXT | | Database override for `--verify-schema` (e.g., PROD) |
| `--snowflake-syntax-check` | | FLAG | Config | Validate SQL expressions against Snowflake |
| `--no-snowflake-check` | | FLAG | | Disable Snowflake syntax validation (overrides config) |

**Note:** The `--target` option here is used to override which database to verify columns against when using `--verify-schema`. It is NOT the same as `--target` in other commands (like `deploy` or `extract`) which specifies the dbt profile target.

---

## Examples

### Basic Validation

```bash
# Validate current directory (run from dbt project root)
sst validate

# Validate with verbose output
sst validate --verbose

# Strict mode (fail on warnings)
sst validate --strict
```

### With dbt Integration

```bash
# Auto-compile dbt manifest if missing/stale
sst validate --dbt-compile

# Use custom dbt target with auto-compile
export DBT_TARGET=ci
sst validate --dbt-compile
```

### With Snowflake Verification

```bash
# Verify columns exist in Snowflake (requires connection)
sst validate --verify-schema

# Verify against specific database
sst validate --verify-schema --target PROD

# Validate SQL syntax (catches typos like CUONT → COUNT)
sst validate --snowflake-syntax-check

# Skip syntax check (overrides config)
sst validate --no-snowflake-check
```

### Custom Paths and Exclusions

```bash
# Custom paths (override config)
sst validate --dbt models/ --semantic semantic_models/

# Exclude directories temporarily
sst validate --exclude temp_models,experimental
```

---

## What Gets Validated

### Table Validation

- Model has `description`
- Model has `primary_key` in `meta.sst`
- `primary_key` is not empty and is a list
- Table name matches model name
- Primary key columns exist in the model
- Table has `synonyms` defined (warning)

### Column Validation

- Column has `description`
- Column has `column_type` (dimension, fact, or time_dimension)
- Column has `data_type` (valid Snowflake type)
- Fact columns have numeric data types
- Time dimension columns have temporal data types
- PII columns have no `sample_values`
- Enum columns have `sample_values`

### Metric Validation

- Metric has `name`, `expr`, `tables`
- `expr` is a non-empty string
- `tables` is a non-empty list
- All referenced tables exist in dbt catalog
- All referenced columns exist in their tables
- No duplicate metric names

### Relationship Validation

- Relationship has `name`, `left_table`, `right_table`, `relationship_conditions`
- Conditions use valid operators (= or >=)
- Left and right tables exist in dbt catalog
- Join columns exist in their respective tables
- Right join columns reference primary key or unique columns
- No circular dependencies

### Filter Validation

- Filter has `name`, `expr`
- All referenced tables and columns exist
- No duplicate filter names

### Semantic View Validation

- View has `name`, `tables`
- All referenced tables exist
- Referenced metrics, relationships, custom instructions exist
- No duplicate table synonyms within a view

### Template Validation

- All `{{ table('name') }}` templates resolve
- All `{{ column('table', 'col') }}` templates resolve
- All `{{ metric('name') }}` templates resolve
- No circular dependencies in metric references

### SQL Syntax Validation (Optional)

When `--snowflake-syntax-check` is enabled:

- Metric `expr` is valid Snowflake SQL
- Filter `expr` is valid Snowflake SQL
- Verified query `sql` is valid Snowflake SQL
- Includes "Did you mean?" suggestions for typos

---

## Configuration

Enable syntax checking by default in `sst_config.yml`:

```yaml
validation:
  # Skip these directories during validation
  exclude_dirs:
    - "_intermediate"
    - "staging"
    - "models/legacy/*"  # Glob patterns supported
  
  # Fail on warnings (strict mode)
  strict: false
  
  # Validate SQL against Snowflake by default
  snowflake_syntax_check: true
```

---

## Output

### Success (Clean)

```
18:15:14  Validating 1188 model(s)
18:15:37  Validation completed with 0 error(s), 0 warning(s)
```

### With Warnings

```
18:15:14  Validating 1188 model(s)
18:15:15  WARNING in INCOMPLETE_MODEL: Table has no synonyms
18:15:16  WARNING in ANOTHER_MODEL: Skipped (missing primary_key)
18:15:17  WARNING in TABLE_C: Missing description
18:15:37  Validation completed with 0 error(s), 3 warning(s)

Warnings:
  [WARNING] Table 'INCOMPLETE_MODEL' has no synonyms defined
  [WARNING] Table 'ANOTHER_MODEL' skipped (missing primary_key)
  [WARNING] Table 'TABLE_C' is missing description
```

### With Errors

```
18:15:14  Validating 1188 model(s)
18:15:15  ERROR in bad_metric: References non-existent table 'sales'
18:15:16  ERROR in broken_relationship: Missing left table
18:15:37  Validation failed with 2 error(s), 0 warning(s)

Errors:
  [ERROR] Metric 'revenue' references non-existent table 'sales'
  [ERROR] Relationship 'orders_to_customers' missing left table
```

---

## Severity Levels

| Severity | Meaning | Blocks Validation |
|----------|---------|-------------------|
| REQUIRED | Must exist before validation runs | Yes (exit code 1) |
| ERROR | Critical issue that prevents generation | Yes (exit code 1) |
| WARNING | Issue that should be reviewed | Only with `--strict` |
| INFO | Suggestion for improvement | No |

---

## Troubleshooting

### "manifest.json not found"

```bash
# Generate manifest first
dbt compile --target prod

# Or use auto-compile flag
sst validate --dbt-compile
```

### "Table not found"

Check the error message for "Did you mean?" suggestions:

```
ERROR: Template {{ table('user_orders') }} could not be resolved
Did you mean: users_orders, customer_orders
```

### "Circular dependency"

Break the cycle by creating base metrics without dependencies:

```yaml
# Instead of: metric_a → metric_b → metric_a
# Create: base_metric (no deps) → metric_a → metric_b
```

### "Duplicate names"

Names are normalized for comparison. These are duplicates:
- `Total_Revenue`, `total_revenue`, `TotalRevenue`

Use prefixes to differentiate:
- `finance_revenue`, `sales_revenue`

### Slow validation

Schema verification and SQL syntax checks require Snowflake connections.

```bash
# Skip Snowflake checks for faster local validation
sst validate --no-snowflake-check
```

---

## CI/CD Integration

```yaml
# Validate on every PR (no Snowflake writes)
- run:
    name: Validate semantic models
    command: sst validate

# Production: validate before deployment
- run:
    name: Deploy
    command: |
      sst validate --strict
      sst deploy --target prod
```

---

## Related

- [Validation Rules](../concepts/validation-rules.md) - Complete list of 99+ checks
- [sst deploy](deploy.md) - Deploy after validation
- [sst format](format.md) - Format YAML before validation
- [CI/CD Guide](../guides/ci-cd.md) - Pipeline integration
