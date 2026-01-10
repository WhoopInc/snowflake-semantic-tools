# CLI Reference

Complete reference for all Snowflake Semantic Tools commands.

---

## Commands Overview

| Command | Description | Snowflake Required |
|---------|-------------|-------------------|
| [`sst init`](init.md) | Interactive setup wizard | Optional |
| [`sst debug`](debug.md) | Show configuration and test connection | Optional |
| [`sst enrich`](enrich.md) | Enrich dbt YAML with metadata from Snowflake | Yes |
| [`sst validate`](validate.md) | Validate semantic models (99+ checks) | No |
| [`sst format`](format.md) | YAML linter for consistency | No |
| [`sst extract`](extract.md) | Load metadata to Snowflake tables | Yes |
| [`sst generate`](generate.md) | Create semantic views | Yes |
| [`sst deploy`](deploy.md) | One-step: validate → extract → generate | Yes |
| [`sst migrate-meta`](migrate-meta.md) | Migrate to dbt Fusion format | No |

---

## Which Command Should I Use?

### Setting Up

| Goal | Command |
|------|---------|
| First time setup | [`sst init`](init.md) |
| Verify configuration | [`sst debug`](debug.md) |
| Test Snowflake connection | [`sst debug --test-connection`](debug.md) |

### Development

| Goal | Command |
|------|---------|
| Add metadata to dbt models | [`sst enrich --models name1,name2`](enrich.md) |
| Generate LLM synonyms | [`sst enrich --models name --synonyms`](enrich.md) |
| Check for errors | [`sst validate`](validate.md) |
| Standardize YAML formatting | [`sst format models/`](format.md) |
| Migrate meta.sst to config.meta.sst | [`sst migrate-meta models/`](migrate-meta.md) |

### Deployment

| Goal | Command |
|------|---------|
| One-step deployment | [`sst deploy`](deploy.md) |
| Load metadata to Snowflake | [`sst extract`](extract.md) |
| Generate semantic views | [`sst generate --all`](generate.md) |
| Generate specific views | [`sst generate -v view_name`](generate.md) |

---

## Commands by Workflow

### New Project Setup

```bash
# 1. Run the setup wizard
sst init

# 2. Test your configuration
sst debug --test-connection

# 3. Enrich your first models
sst enrich --models customers,orders
```

### Daily Development

```bash
# Add metadata to models you're working on
sst enrich --models my_model

# Validate before committing
sst validate

# Format for consistency
sst format models/
```

### Deploying to Production

```bash
# Option A: One-step (recommended)
sst deploy --target prod

# Option B: Step-by-step
sst validate
sst extract --target prod
sst generate --target prod --all
```

### CI/CD Pipeline

```bash
# On every PR: validate only
sst validate

# On merge to main: full deployment
sst deploy --target prod
```

### Incremental Development with Defer

```bash
# Generate only changed views (references prod tables)
sst generate --all --defer-target prod --only-modified

# Full deployment with defer
sst deploy --defer-target prod --only-modified
```

---

## Common Options

These options are available across multiple commands:

| Option | Commands | Description |
|--------|----------|-------------|
| `--target`, `-t` | Most commands | dbt target from profiles.yml |
| `--verbose`, `-v` | Most commands | Show detailed output |
| `--dry-run` | enrich, format, generate | Preview changes without writing |
| `--exclude` | validate, enrich | Comma-separated directories to skip |

---

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `DBT_TARGET` | Default dbt target (used by `--dbt-compile`) |
| `SNOWFLAKE_*` | Snowflake credentials (see [Authentication](../guides/authentication.md)) |

---

## Configuration

Most command behavior can be configured in `sst_config.yaml`:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

validation:
  exclude_dirs: ["_intermediate", "staging"]
  strict: false
  snowflake_syntax_check: true

enrichment:
  synonym_model: 'mistral-large2'
  synonym_max_count: 4

defer:
  target: prod
  state_path: ./prod_run_artifacts
```

Full reference: [Configuration Reference](../reference/config.md)

---

## Prerequisites

Before using the CLI:

1. **Installation:** `pip install snowflake-semantic-tools`
2. **Configuration:** Create `sst_config.yaml` in your dbt project root
3. **Authentication:** Configure `~/.dbt/profiles.yml` for Snowflake

See [Getting Started](../getting-started.md) for detailed setup instructions.

---

## Command Reference

### Setup Commands

- **[sst init](init.md)** - Interactive setup wizard for configuring SST in a dbt project
- **[sst debug](debug.md)** - Show configuration and optionally test Snowflake connection

### Development Commands

- **[sst enrich](enrich.md)** - Enrich dbt YAML metadata with semantic information from Snowflake
- **[sst validate](validate.md)** - Validate semantic models against dbt definitions (99+ checks)
- **[sst format](format.md)** - YAML linter for project-wide formatting consistency

### Deployment Commands

- **[sst extract](extract.md)** - Extract metadata from dbt/semantic models to Snowflake tables
- **[sst generate](generate.md)** - Generate Snowflake SEMANTIC VIEWs from metadata tables
- **[sst deploy](deploy.md)** - One-step deployment: validate → extract → generate

### Utility Commands

- **[sst migrate-meta](migrate-meta.md)** - Migrate meta.sst to config.meta.sst (dbt Fusion compatibility)

---

## Related

- [Getting Started](../getting-started.md) - Installation and first steps
- [Configuration Reference](../reference/config.md) - sst_config.yaml options
- [Authentication Guide](../guides/authentication.md) - Snowflake connection setup
- [Quick Reference](../reference/quick-reference.md) - CLI cheat sheet
