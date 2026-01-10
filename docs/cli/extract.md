# sst extract

Extract metadata from dbt and semantic models to Snowflake tables.

---

## Overview

The `extract` command parses your dbt YAML files and semantic model definitions, then loads the extracted metadata into Snowflake tables. These tables are then used by `sst generate` to create semantic views.

**Snowflake Connection:** Required

---

## Quick Start

```bash
# Extract using profile defaults
sst extract

# Extract to specific target
sst extract --target prod

# Override database/schema
sst extract --db PROD_DB --schema SEMANTIC_METADATA
```

---

## Syntax

```bash
sst extract [OPTIONS]
```

---

## Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--target` | `-t` | TEXT | Profile default | dbt target from profiles.yml |
| `--db` | | TEXT | From profile | Target database for metadata tables |
| `--schema` | `-s` | TEXT | From profile | Target schema for metadata tables |
| `--dbt` | | PATH | Auto-detect | Path to dbt models directory |
| `--semantic` | | PATH | Auto-detect | Path to semantic models directory |
| `--verbose` | `-v` | FLAG | False | Show detailed extraction progress |

---

## What It Creates

The extract command creates or updates these tables in your target schema:

| Table | Contents |
|-------|----------|
| `SM_METRICS` | Metric definitions |
| `SM_RELATIONSHIPS` | Table relationships |
| `SM_RELATIONSHIP_COLUMNS` | Relationship column mappings |
| `SM_DIMENSIONS` | Dimension columns |
| `SM_FACTS` | Fact columns |
| `SM_FILTERS` | Filter definitions |
| `SM_VERIFIED_QUERIES` | Verified query examples |
| `SM_CUSTOM_INSTRUCTIONS` | Custom AI instructions |
| `SM_SEMANTIC_VIEWS` | Semantic view definitions |
| `SM_TABLE_SUMMARIES` | Table metadata summaries |

---

## Cortex Search Service

The extract command automatically creates or updates a Cortex Search Service named `SEMANTIC_SEARCH_SERVICE` that indexes the `SM_TABLE_SUMMARIES` table.

This service enables AI-powered table discovery for dynamic model generation, but **only includes tables where `cortex_searchable: true`** in the dbt model metadata.

---

## Examples

```bash
# Simplest: use profile defaults for database/schema
sst extract

# Use specific dbt target
sst extract --target prod

# Override database/schema from profile
sst extract --db PROD_DB --schema SEMANTIC_METADATA

# With verbose output
sst extract --target dev --verbose

# Custom paths (override config)
sst extract --dbt models/ --semantic semantic_models/
```

---

## Output

```
Starting semantic metadata extraction...
Successfully extracted 3942 models

Tables populated:
  SM_METRICS: 177 rows
  SM_RELATIONSHIPS: 44 rows
  SM_DIMENSIONS: 2531 rows
  SM_FACTS: 554 rows
  SM_FILTERS: 12 rows
  SM_VERIFIED_QUERIES: 25 rows
  SM_CUSTOM_INSTRUCTIONS: 3 rows
  SM_SEMANTIC_VIEWS: 5 rows
  SM_TABLE_SUMMARIES: 3942 rows
```

---

## Configuration

Database and schema default to values from your dbt `profiles.yml`:

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      database: DEV_DB
      schema: SEMANTIC
    prod:
      database: PROD_DB
      schema: SEMANTIC
```

Then use targets to control extraction destination:

```bash
sst extract --target dev   # → DEV_DB.SEMANTIC
sst extract --target prod  # → PROD_DB.SEMANTIC
```

---

## Workflow

Extract is typically run before generate:

```
┌─────────────────┐
│    EXTRACT      │  Parse YAML, load to Snowflake tables
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    GENERATE     │  Create semantic views from tables
└─────────────────┘
```

Or use `sst deploy` to run validate → extract → generate in one command.

---

## Troubleshooting

### "Permission denied"

Your Snowflake role needs these permissions:

```sql
GRANT USAGE ON DATABASE database_name TO ROLE role_name;
GRANT CREATE SCHEMA ON DATABASE database_name TO ROLE role_name;
GRANT CREATE TABLE ON SCHEMA schema_name TO ROLE role_name;
```

### "No models found"

Check that `sst_config.yaml` paths are correct:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
```

### Tables not updating

Extraction fully replaces table contents. If tables seem stale:

1. Check you're using the correct target
2. Verify the extraction completed successfully
3. Check for errors in verbose output

---

## Related

- [sst generate](generate.md) - Generate semantic views (run after extract)
- [sst deploy](deploy.md) - One-step deployment
- [sst validate](validate.md) - Validate before extracting
