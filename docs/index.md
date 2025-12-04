# Snowflake Semantic Tools

**dbt extension for managing Snowflake Semantic Views and Cortex Analyst semantic models**

---

## What is SST?

SST integrates Snowflake's semantic layer directly into your dbt project. Instead of managing semantic views separately in Snowflake, you define them in YAML alongside your dbt models and deploy them as native Snowflake SEMANTIC VIEW objects.

**The workflow:**

1. **Define in dbt** - Write metrics, relationships, and semantic views as YAML in your dbt repo
2. **Enrich from Snowflake** - Auto-populate metadata (column types, samples) from your tables
3. **Validate locally** - Catch errors before touching Snowflake
4. **Deploy to Snowflake** - Create SEMANTIC VIEW objects for Cortex Analyst and BI tools
5. **Version control** - Track changes to your semantic layer in Git with dbt

**Snowflake features supported:**

- **SEMANTIC VIEWs** - Native Snowflake objects for semantic queries
- **Cortex Analyst** - AI-powered natural language analytics
- **Semantic models** - YAML definitions for LLM consumption

---

## Installation

```bash
pip install snowflake-semantic-tools
```

---

## Quick Start

### 1. Configure

Create `sst_config.yml` in your dbt project:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

validation:
  exclude_dirs:
    - "_intermediate"                    # Excludes any dir named "_intermediate"
    - "staging"                          # Excludes any dir named "staging"
    - "models/legacy/*"                  # Glob pattern: specific path
```

Create `.env` for credentials (optional - browser SSO works without password):

```bash
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_USER=your.email@company.com
SNOWFLAKE_WAREHOUSE=MY_WAREHOUSE
SNOWFLAKE_ROLE=MY_ROLE
# Leave SNOWFLAKE_PASSWORD blank for browser SSO (recommended for local)
```

### 2. Use Commands

```bash
# Validate models
sst validate

# Enrich with metadata
sst enrich models/ --database PROD_DB --schema my_schema

# Deploy to Snowflake
sst deploy --db PROD_DB --schema SEMANTIC_VIEWS
```

---

## Core Features

### Validate
Check semantic models for errors without Snowflake connection.

```bash
sst validate
```

### Enrich
Populate YAML files with metadata from Snowflake automatically.

```bash
sst enrich models/ --database PROD_DB --schema my_schema
```

### Format
YAML linter for project-wide consistency.

```bash
sst format models/
```

### Extract
Load metadata to Snowflake tables.

```bash
sst extract --db PROD_DB --schema METADATA
```

### Generate
Create Snowflake SEMANTIC VIEWs.

```bash
sst generate --metadata-db PROD_DB --metadata-schema METADATA \
             --target-db PROD_DB --target-schema VIEWS --all
```

### Local Development (Like dbt defer)
Generate dev views that reference production tables.

```bash
# Metadata in SCRATCH, views reference ANALYTICS tables
sst generate --metadata-db SCRATCH --metadata-schema my_test \
             --target-db SCRATCH --target-schema my_test \
             --defer-database ANALYTICS --all
```

Perfect for testing metadata changes locally without rebuilding all dbt models!

### Deploy
One-step: validate → extract → generate.

```bash
sst deploy --db PROD_DB --schema SEMANTIC_VIEWS
```

---

## Example Output

Clean, professional logging:

```
18:20:00  Enriching 5 model(s) in models/customers
18:20:01   1 of  5  customer_status_daily          [OK in 2.3s]
18:20:03   2 of  5  customer_lifecycle             [OK in 1.8s]
18:20:05   3 of  5  customer_summary               [OK in 2.1s]
18:20:07   4 of  5  customer_transactions          [OK in 1.5s]
18:20:08   5 of  5  customer_preferences           [OK in 1.9s]
18:20:08  Completed in 8.0s (5 enriched)
```

---

## Documentation Guide

### Getting Started
- **[Getting Started](getting-started.md)** - Installation, configuration, first commands
- **[Authentication](authentication.md)** - Snowflake connection setup

### Learn Features
- **[User Guide](user-guide.md)** - How enrichment and validation work
- **[Semantic Models Guide](semantic-models-guide.md)** - Writing metrics and relationships

### Reference
- **[Validation Checklist](validation-checklist.md)** - Complete list of all 98 validation checks
- **[CLI Reference](cli-reference.md)** - All command options
- **[API Reference](api-reference.md)** - Python API

---

## Requirements

- Python 3.9+
- Snowflake account
- dbt project with YAML definitions

---

## Support

- **Issues**: GitHub Issues
- **Changelog**: [CHANGELOG.md](../CHANGELOG.md)

---

Built for the Snowflake and dbt communities.
