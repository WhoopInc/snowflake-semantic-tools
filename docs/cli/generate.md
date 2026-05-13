# sst generate

Generate Snowflake Semantic Views from compiled metadata.

---

## Overview

The `generate` command creates Snowflake SEMANTIC VIEW objects from the local `sst_manifest.json` compiled by `sst compile`. It supports defer mode for referencing production tables from development environments and selective generation for fast iteration.

By default, `generate` reads from the local manifest (`target/sst_manifest.json`). Use `--from-snowflake` to read from SM_* metadata tables instead (legacy workflow).

**Snowflake Connection:** Required (to execute DDL)

---

## Quick Start

```bash
# Generate all semantic views
sst generate --all

# Generate specific views
sst generate -v customer_360 -v sales_summary

# Generate with defer to production
sst generate --all --defer-target prod
```

---

## Syntax

```bash
sst generate [OPTIONS]
```

---

## Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--target` | `-t` | TEXT | Profile default | dbt target from profiles.yml |
| `--db` | | TEXT | From profile | Target database for views |
| `--schema` | `-s` | TEXT | From profile | Target schema for views |
| `--views` | `-v` | TEXT | | Specific views to generate (repeatable) |
| `--all` | `-a` | FLAG | False | Generate all available views |
| `--defer-target` | | TEXT | | dbt target for table references |
| `--state` | | PATH | | Path to defer state artifacts directory |
| `--only-modified` | | FLAG | False | Only generate changed views (requires defer) |
| `--no-defer` | | FLAG | False | Disable defer (overrides config) |
| `--dry-run` | | FLAG | False | Generate SQL to files without executing |
| `--output-dir` | | PATH | `target/semantic_views/` | Output directory for dry-run SQL files |
| `--from-snowflake` | | FLAG | False | Read metadata from SM_* tables instead of manifest |
| `--threads` | | INT | From config or 1 | Concurrent view generation threads |
| `--verbose` | | FLAG | False | Show detailed progress |

**Note:** Either `--views` or `--all` must be provided.

---

## Examples

### Basic Usage

```bash
# Generate all semantic views
sst generate --all

# Generate specific views
sst generate -v customer_360 -v sales_summary

# Use specific dbt target
sst generate --target prod --all

# Override database/schema
sst generate --db ANALYTICS --schema SEMANTIC --all
```

### With Defer Mode

```bash
# Generate views that reference production tables
sst generate --all --defer-target prod

# Selective generation (only changed views)
sst generate --all --defer-target prod --only-modified

# dbt Cloud CLI with explicit state path
sst generate --all --defer-target prod --state ./prod_artifacts
```

### Preview and Debug

```bash
# Generate SQL files without executing (writes to target/semantic_views/)
sst generate --all --dry-run

# Write SQL files to a custom directory
sst generate --all --dry-run --output-dir ./review/sql/

# Verbose output
sst generate --all --verbose
```

Dry-run writes one `.sql` file per view to the output directory. Use this to inspect generated DDL, diff between runs, or review changes before deploying.

---

## Defer Mode

The defer feature allows you to generate semantic views that reference tables from a different environment (e.g., production) while working in development. This is similar to dbt's `--defer` flag.

### How It Works

1. SST reads table locations from a "defer manifest" (compiled for prod target)
2. Generated views reference tables at the locations specified in that manifest
3. Your development environment doesn't need the actual tables

### dbt Core Users

```bash
# Step 1: Compile the production manifest
dbt compile --target prod

# Step 2: Generate views with defer
sst generate --all --defer-target prod
```

### dbt Cloud CLI Users

```bash
# Step 1: Download manifest.json from dbt Cloud job artifacts
# Place it in ./prod_run_artifacts/manifest.json

# Step 2: Generate with explicit state path
sst generate --all --defer-target prod --state ./prod_run_artifacts
```

---

## Selective Generation

Use `--only-modified` to regenerate only views affected by changes:

```bash
sst generate --all --defer-target prod --only-modified
```

This detects two categories of changes and regenerates only the impacted views:

1. **dbt model SQL changes** — compares `manifest.json` checksums between current and defer manifests
2. **SST YAML changes** — compares `sst_manifest.json` checksums for all SST-relevant YAML files

### What Gets Detected

| Change Type | Detected? | Views Regenerated |
|-------------|-----------|-------------------|
| dbt model (`.sql`) | Yes | Views referencing that table |
| dbt model YAML (facts/dimensions) | Yes | Views referencing that table |
| Metrics YAML | Yes | All views (cross-cutting) |
| Relationships YAML | Yes | All views (cross-cutting) |
| Filters YAML | Yes | All views (cross-cutting) |
| Verified queries YAML | Yes | All views (cross-cutting) |
| Custom instructions YAML | Yes | All views (cross-cutting) |
| Semantic view definition | Yes | Views defined in that file |
| `sst_config.yml` | Yes | All views (config is global) |

### SST Manifest

After each successful non-dry-run generation, SST saves `sst_manifest.json` alongside dbt's `manifest.json` in `./target/`. This file contains SHA-256 checksums of all SST-relevant YAML files and is used as the baseline for the next `--only-modified` comparison.

When using `--state` with external artifacts, ensure both `manifest.json` and `sst_manifest.json` are present. If `sst_manifest.json` is missing (SST-G007), all SST YAML files are treated as changed.

### Example Output

```
Detected changes:
  dbt models: 0 changed
  SST YAML: 1 file(s) changed
    • models/marts/customers.yml
Will regenerate 2 view(s)
Filtering to 2 view(s): jaffle_shop_complete, jaffle_shop_sales_analytics
```

---

## Configuration

Set defer defaults in `sst_config.yml`:

```yaml
defer:
  # Default target for defer operations
  target: prod
  
  # Path to defer state artifacts (required for dbt Cloud CLI)
  state_path: ./prod_run_artifacts
  
  # Auto-compile manifest (dbt Core only)
  auto_compile: false
```

With this config, `sst generate --all` automatically uses prod defer.

---

## Cortex Analyst Metadata

When generating semantic views, SST automatically includes a `WITH EXTENSION (CA='...')` clause containing:

- `sample_values` for dimensions, time_dimensions, and facts
- `is_enum: true` for columns where sample_values is exhaustive

This metadata helps Cortex Analyst:
- Understand valid categorical values
- Recognize enums for accurate query generation
- Generate more accurate SQL queries

### CA Extension Structure

```json
{
  "tables": [{
    "name": "CUSTOMERS",
    "dimensions": [
      {"name": "CUSTOMER_TYPE", "sample_values": ["new", "returning"], "is_enum": true}
    ],
    "time_dimensions": [
      {"name": "CREATED_AT", "sample_values": ["2025-01-15", "2025-02-20"]}
    ],
    "facts": [
      {"name": "LIFETIME_SPEND", "sample_values": ["100.50", "250.00"]}
    ]
  }]
}
```

---

## Generated DDL Features

The `generate` command emits full Snowflake semantic view DDL with support for:

| Feature | DDL Clause | Source |
|---------|-----------|--------|
| Tables with primary/unique keys | `PRIMARY KEY (...)`, `UNIQUE (...)` | dbt model `config.meta.sst` |
| Non-overlapping ranges | `CONSTRAINT name DISTINCT RANGE BETWEEN start AND end EXCLUSIVE` | Table `constraints` config |
| Tags (table/column level) | `WITH TAG (tag_name = 'value')` | Table/column `tags` config |
| Synonyms (tables, facts, dimensions) | `WITH SYNONYMS = ('alias1', 'alias2')` | `synonyms` field |
| Comments | `COMMENT = '...'` | `description` field |
| Private facts/metrics | `PRIVATE fact_name AS ...` | `visibility: private` |
| Semi-additive metrics | `NON ADDITIVE BY (dim ORDER NULLS)` | Metric `non_additive_by` |
| Join path disambiguation | `USING (relationship_name)` | Metric `using_relationships` |
| Window functions | `OVER (PARTITION BY ... ORDER BY ...)` | Metric `window` config |
| ASOF joins | `REFERENCES table (col, ASOF time_col)` | `>=` in relationship conditions |
| Range joins | `REFERENCES table (BETWEEN start AND end EXCLUSIVE)` | `BETWEEN` in relationship conditions |
| Verified queries | `AI_VERIFIED_QUERIES (name AS (...))` | `snowflake_verified_queries` YAML |
| Filters as instructions | Appended to `AI_SQL_GENERATION` clause | `snowflake_filters` YAML |
| Custom instructions | `AI_SQL_GENERATION`, `AI_QUESTION_CATEGORIZATION` | `snowflake_custom_instructions` |
| Cortex Analyst extension | `WITH EXTENSION (CA='...')` | Column `sample_values` + `is_enum` |

> **Note:** Filter-to-instruction conversion is controlled by the `generation.filters_to_instructions` config option (default: `true`). See [Configuration Reference](../reference/config.md).

---

## Output

```
Generating semantic views...
  [1/3] customer_360          [OK in 2.1s]
  [2/3] sales_summary         [OK in 1.8s]
  [3/3] product_analytics     [OK in 1.5s]

Generated 3 views in 5.4s
```

---

## Troubleshooting

### "No views to generate"

Ensure you have semantic views defined in your semantic models directory:

```yaml
# snowflake_semantic_models/semantic_views.yml
semantic_views:
  - name: customer_360
    description: Customer analytics view
    tables:
      - {{ table('customers') }}
      - {{ table('orders') }}
```

### "Table not found in manifest"

The defer manifest doesn't contain the referenced table.

```bash
# Regenerate the defer manifest
dbt compile --target prod

# Try again
sst generate --all --defer-target prod
```

### "Permission denied"

Your Snowflake role doesn't have CREATE SEMANTIC VIEW privileges.

```sql
GRANT CREATE SEMANTIC VIEW ON SCHEMA schema_name TO ROLE role_name;
```

### Views not updating

If you modified SST YAML files (metrics, relationships, filters), re-compile first:

```bash
sst compile
sst generate --all
```

---

## Metadata Sources

By default, `generate` reads from `target/sst_manifest.json` (created by `sst compile`). This is the recommended workflow — no Snowflake connection is needed for metadata.

Use `--from-snowflake` to read from SM_* metadata tables instead (legacy workflow, requires `sst extract` first):

```bash
# Manifest-first (default, recommended)
sst compile
sst generate --all

# Legacy SM_* table workflow
sst extract
sst generate --all --from-snowflake
```

---

## Workflow

```
┌─────────────────┐
│    COMPILE      │  Parse YAML → target/sst_manifest.json
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    GENERATE     │  Create semantic views from manifest
└─────────────────┘
```

**Tip:** Use `sst deploy` to run compile → validate → generate in one command.

---

## Related

- [sst compile](compile.md) - Compile metadata into local manifest
- [sst deploy](deploy.md) - One-step deployment
- [sst validate](validate.md) - Validate before generating
- [Semantic Models Guide](../concepts/semantic-models.md) - Define semantic views
