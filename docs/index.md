# Snowflake Semantic Tools Documentation

Build and deploy Snowflake Semantic Views from your dbt project.

---

## What is SST?

Snowflake Semantic Tools (SST) helps you create **Snowflake Semantic Views**—a standardized semantic layer that lives in Snowflake and powers AI and BI tools—all from within your dbt projects.

**Key capabilities:**

- **Define semantics as code** - Metrics, relationships, filters, and verified queries as YAML
- **Enrich automatically** - Pull column types, sample values, and metadata from Snowflake
- **Validate before deploy** - 100+ validation rules catch errors before they reach Snowflake
- **Deploy to Snowflake** - Generate native SEMANTIC VIEW objects from your definitions

---

## Documentation Map

| Section | Description | Start Here If... |
|---------|-------------|------------------|
| [Getting Started](getting-started.md) | Installation and first steps | You're new to SST |
| [CLI Reference](cli/index.md) | All commands and options | You need command syntax |
| [Concepts](concepts/semantic-models.md) | Semantic models, validation rules | You're writing semantic models |
| [Guides](guides/authentication.md) | Authentication, CI/CD, migrations | You're setting up infrastructure |
| [Reference](reference/config.md) | Configuration, API, quick reference | You need detailed specifications |

---

## Quick Navigation by Task

### I want to...

| Task | Go To |
|------|-------|
| **Install SST and set up my project** | [Getting Started](getting-started.md) |
| **Enrich my dbt models with metadata** | [sst enrich](cli/enrich.md) |
| **Validate my semantic models** | [sst validate](cli/validate.md) |
| **Deploy semantic views to Snowflake** | [sst deploy](cli/deploy.md) |
| **Write metrics, relationships, filters** | [Semantic Models Guide](concepts/semantic-models.md) |
| **Set up Snowflake authentication** | [Authentication Guide](guides/authentication.md) |
| **Configure CI/CD pipelines** | [CI/CD Guide](guides/ci-cd.md) |
| **Migrate to dbt Fusion format** | [dbt Fusion Migration](guides/dbt-fusion-migration.md) |
| **Use the Python API** | [API Reference](reference/api.md) |
| **Configure sst_config.yaml** | [Configuration Reference](reference/config.md) |

---

## CLI Commands Overview

| Command | Description | Snowflake Required |
|---------|-------------|-------------------|
| [`sst init`](cli/init.md) | Interactive setup wizard | Optional |
| [`sst debug`](cli/debug.md) | Show configuration and test connection | Optional |
| [`sst enrich`](cli/enrich.md) | Enrich dbt YAML with metadata from Snowflake | Yes |
| [`sst validate`](cli/validate.md) | Validate semantic models (99+ checks) | No |
| [`sst format`](cli/format.md) | YAML linter for consistency | No |
| [`sst extract`](cli/extract.md) | Load metadata to Snowflake tables | Yes |
| [`sst generate`](cli/generate.md) | Create semantic views | Yes |
| [`sst deploy`](cli/deploy.md) | One-step: validate → extract → generate | Yes |
| [`sst migrate-meta`](cli/migrate-meta.md) | Migrate to dbt Fusion format | No |

**New to SST?** Start with `sst init` to configure your project.

**Production deployments?** Use `sst deploy` which orchestrates the full workflow.

---

## Common Workflows

### Development Workflow

```bash
# 1. Set up your project (one time)
sst init

# 2. Enrich models with metadata from Snowflake
sst enrich --models customers,orders

# 3. Validate before committing
sst validate

# 4. Format YAML for consistency
sst format models/
```

### Deployment Workflow

```bash
# Option A: One-step deployment
sst deploy --target prod

# Option B: Step-by-step (for debugging)
sst validate
sst extract --target prod
sst generate --target prod --all
```

### CI/CD Workflow

```bash
# PRs: Validate only (no Snowflake writes)
sst validate

# Main branch: Full deployment
sst deploy --target prod
```

---

## Key Concepts

### Semantic Models

Semantic models define the business layer on top of your dbt models:

- **Metrics** - Aggregated calculations (revenue, counts, averages)
- **Relationships** - How tables join together
- **Filters** - Reusable WHERE conditions
- **Custom Instructions** - Business rules for Cortex Analyst
- **Verified Queries** - Example queries for AI training
- **Semantic Views** - Curated combinations of the above

Learn more: [Semantic Models Guide](concepts/semantic-models.md)

### Template System

All semantic models use templates that reference your dbt models:

```yaml
# Reference a table
{{ table('orders') }}

# Reference a column
{{ column('orders', 'customer_id') }}

# Reference another metric
{{ metric('total_revenue') }}
```

Templates are validated against your dbt catalog to catch errors early.

### Validation

SST includes 99+ validation checks covering:

- Table and column references
- Template resolution
- Circular dependencies
- SQL syntax (optional Snowflake connection)
- Duplicate detection

Learn more: [Validation Rules](concepts/validation-rules.md)

---

## Configuration

SST is configured via `sst_config.yaml` in your dbt project root:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"

validation:
  exclude_dirs: []
  strict: false
  snowflake_syntax_check: true

enrichment:
  distinct_limit: 25
  sample_values_display_limit: 10
  synonym_model: 'mistral-large2'
```

**Note:** The dbt models directory is auto-detected from `dbt_project.yml`.

Full reference: [Configuration Reference](reference/config.md)

---

## Getting Help

- **Documentation:** You're here! Use the navigation above.
- **Issues:** [GitHub Issues](https://github.com/WhoopInc/snowflake-semantic-tools/issues)
- **Quick Reference:** [CLI Cheat Sheet](reference/quick-reference.md)

---

## Document Index

### Getting Started
- [Getting Started](getting-started.md) - Installation and first steps

### CLI Reference
- [CLI Overview](cli/index.md) - Command navigation and workflows
- [sst init](cli/init.md) - Interactive setup wizard
- [sst debug](cli/debug.md) - Configuration and connection testing
- [sst enrich](cli/enrich.md) - Metadata enrichment
- [sst validate](cli/validate.md) - Semantic model validation
- [sst format](cli/format.md) - YAML formatting
- [sst extract](cli/extract.md) - Metadata extraction
- [sst generate](cli/generate.md) - Semantic view generation
- [sst deploy](cli/deploy.md) - One-step deployment
- [sst migrate-meta](cli/migrate-meta.md) - dbt Fusion migration

### Concepts
- [Semantic Models](concepts/semantic-models.md) - Writing metrics, relationships, filters
- [Validation Rules](concepts/validation-rules.md) - Complete validation checklist

### Guides
- [Authentication](guides/authentication.md) - Snowflake connection setup
- [CI/CD](guides/ci-cd.md) - Pipeline integration
- [dbt Fusion Migration](guides/dbt-fusion-migration.md) - Meta format migration

### Reference
- [Configuration](reference/config.md) - sst_config.yaml reference
- [API](reference/api.md) - Python API reference
- [Quick Reference](reference/quick-reference.md) - CLI cheat sheet
