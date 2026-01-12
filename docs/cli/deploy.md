# sst deploy

One-step deployment: validate → extract → generate.

---

## Overview

The `deploy` command orchestrates the complete SST workflow in a single command. It validates your semantic models, extracts metadata to Snowflake, and generates semantic views. Use this for production deployments or when you want a simple, reliable deployment process.

**Snowflake Connection:** Required

---

## Quick Start

```bash
# Deploy using profile defaults
sst deploy

# Deploy to production
sst deploy --target prod

# Deploy with defer to production tables
sst deploy --defer-target prod
```

---

## Syntax

```bash
sst deploy [OPTIONS]
```

---

## Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--target` | `-t` | TEXT | Profile default | dbt target from profiles.yml |
| `--db` | | TEXT | From profile | Target database |
| `--schema` | `-s` | TEXT | From profile | Target schema |
| `--defer-target` | | TEXT | | dbt target for table references |
| `--state` | | PATH | | Path to defer state artifacts directory |
| `--only-modified` | | FLAG | False | Only generate changed views (requires defer) |
| `--no-defer` | | FLAG | False | Disable defer (overrides config) |
| `--skip-validation` | | FLAG | False | Skip validation step |
| `--verbose` | `-v` | FLAG | False | Show detailed progress |
| `--quiet` | `-q` | FLAG | False | Show errors and warnings only |

---

## What It Does

The deploy command executes three steps in sequence:

```
┌─────────────────┐
│    VALIDATE     │  Check for errors (fail fast)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    EXTRACT      │  Load metadata to Snowflake
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    GENERATE     │  Create semantic views
└─────────────────┘
```

**Stops at first failure:** If validation fails, extraction is skipped. If extraction fails, generation is skipped.

---

## Examples

### Basic Usage

```bash
# Deploy using profile defaults
sst deploy

# Deploy to specific target
sst deploy --target prod

# Override database/schema
sst deploy --db QA_DB --schema SEMANTIC_VIEWS
```

### With Defer Mode

```bash
# Deploy to dev, but views reference prod tables
sst deploy --defer-target prod

# Selective deployment (only changed views)
sst deploy --defer-target prod --only-modified

# dbt Cloud CLI with explicit state
sst deploy --defer-target prod --state ./prod_artifacts
```

### Production Options

```bash
# Skip validation (already ran in CI)
sst deploy --skip-validation

# Quiet mode (errors only)
sst deploy --quiet
```

---

## Configuration

Set defer defaults in `sst_config.yml`:

```yaml
defer:
  target: prod
  state_path: ./prod_run_artifacts
```

With this config, `sst deploy` automatically uses prod defer.

---

## Output

```
Step 1/3: Validating semantic models...
Validation passed (0 errors, 5 warnings)

Step 2/3: Extracting metadata to Snowflake...
Extraction completed (1,188 models)

Step 3/3: Generating semantic artifacts...
Generated 3 views

Deployment completed successfully in 45.2s
```

---

## When to Use Deploy vs Individual Commands

| Scenario | Recommendation |
|----------|----------------|
| Production deployment | `sst deploy` |
| CI/CD pipeline | `sst deploy` |
| Debugging issues | Individual commands |
| First-time setup | Individual commands |
| Iterating on changes | Individual commands |

**For debugging**, run commands separately:

```bash
sst validate --verbose
sst extract --verbose
sst generate --all --verbose
```

---

## CI/CD Integration

```yaml
# CircleCI example
jobs:
  deploy-semantic-models:
    steps:
      - run:
          name: Deploy semantic models
          command: sst deploy --target prod

workflows:
  semantic-models:
    jobs:
      - deploy-semantic-models:
          filters:
            branches:
              only: main
```

See [CI/CD Guide](../guides/ci-cd.md) for complete examples.

---

## Troubleshooting

### "Validation failed"

Fix validation errors before deployment:

```bash
# See detailed errors
sst validate --verbose

# Fix errors, then retry
sst deploy
```

### "Extraction failed"

Check Snowflake connection and permissions:

```bash
# Test connection
sst debug --test-connection

# Check role permissions
# Need CREATE TABLE, CREATE SCHEMA permissions
```

### "Generation failed"

Usually a permissions issue:

```sql
-- Grant semantic view creation
GRANT CREATE SEMANTIC VIEW ON SCHEMA schema_name TO ROLE role_name;
```

### Deployment is slow

Use selective generation with defer:

```bash
sst deploy --defer-target prod --only-modified
```

---

## Related

- [sst validate](validate.md) - Validate semantic models
- [sst extract](extract.md) - Extract metadata to Snowflake
- [sst generate](generate.md) - Generate semantic views
- [CI/CD Guide](../guides/ci-cd.md) - Pipeline integration
