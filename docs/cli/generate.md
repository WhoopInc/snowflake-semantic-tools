# sst generate

Generate Snowflake Semantic Views from metadata tables.

---

## Overview

The `generate` command creates Snowflake SEMANTIC VIEW objects from the metadata previously loaded by `sst extract`. It supports defer mode for referencing production tables from development environments and selective generation for fast iteration.

**Snowflake Connection:** Required

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
| `--dry-run` | | FLAG | False | Preview without executing |
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
# Preview SQL without executing
sst generate --all --dry-run

# Verbose output
sst generate --all --verbose
```

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

Use `--only-modified` to regenerate only views affected by dbt model changes:

```bash
sst generate --all --defer-target prod --only-modified
```

This compares dbt model checksums between your current `manifest.json` and the defer manifest, then regenerates only semantic views that reference the changed models.

### What Gets Detected

| Change Type | Detected by `--only-modified`? | Action Required |
|-------------|-------------------------------|-----------------|
| dbt model (`.sql`) | Yes | Automatic |
| Metrics YAML | No | Run `sst extract` + `sst generate --all` |
| Relationships YAML | No | Run `sst extract` + `sst generate --all` |
| Filters YAML | No | Run `sst extract` + `sst generate --all` |
| Semantic view definition | No | Run `sst generate --all` |

**Important:** This flag only detects changes to dbt models (`.sql` files), not changes to SST YAML files.

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

If you modified SST YAML files (metrics, relationships, filters), you need to re-extract:

```bash
sst extract --target prod
sst generate --all --target prod
```

---

## Workflow

```
┌─────────────────┐
│    EXTRACT      │  Load metadata to Snowflake tables
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    GENERATE     │  Create semantic views from metadata
└─────────────────┘
```

**Tip:** Use `sst deploy` to run validate → extract → generate in one command.

---

## Related

- [sst extract](extract.md) - Load metadata to Snowflake (run before generate)
- [sst deploy](deploy.md) - One-step deployment
- [sst validate](validate.md) - Validate before generating
- [Semantic Models Guide](../concepts/semantic-models.md) - Define semantic views
