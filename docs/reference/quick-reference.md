# CLI Quick Reference

One-page cheat sheet for all SST commands.

---

## Setup Commands

```bash
# Interactive setup wizard
sst init

# Non-interactive setup
sst init --skip-prompts

# Check setup status
sst init --check-only

# Show configuration
sst debug

# Test Snowflake connection
sst debug --test-connection
sst debug --target prod --test-connection
```

---

## Development Commands

### Enrich

```bash
# Enrich by model name (requires manifest)
sst enrich --models name1,name2
sst enrich -m name1,name2

# Enrich by path
sst enrich models/directory/
sst enrich path/to/file.yml

# Enrich with wildcard patterns
sst enrich "models/analytics/shared_prefix_*"
sst enrich "models/analytics/_intermediate/*"

# Component selection
sst enrich -m name -ct              # Column types only
sst enrich -m name -dt              # Data types only
sst enrich -m name -sv              # Sample values only
sst enrich -m name --synonyms       # Generate synonyms
sst enrich -m name --all            # All components

# Force overwrite
sst enrich -m name --force-synonyms
sst enrich -m name --force-all

# Preview
sst enrich -m name --dry-run
```

### Validate

```bash
# Basic validation
sst validate

# Auto-compile manifest
sst validate --dbt-compile

# Snowflake verification
sst validate --verify-schema
sst validate --snowflake-syntax-check

# Strict mode (warnings = failures)
sst validate --strict

# Exclude directories
sst validate --exclude temp,experimental
```

### Format

```bash
# Format files
sst format models/
sst format path/to/file.yml

# Format with wildcard patterns
sst format "models/analytics/shared_prefix_*"
sst format "models/analytics/_intermediate/*"

# Check formatting (for CI)
sst format models/ --check

# Preview changes
sst format models/ --dry-run

# Sanitize problematic characters
sst format models/ --sanitize
```

---

## Deployment Commands

### Extract

```bash
# Extract to default target
sst extract

# Extract to specific target
sst extract --target prod

# Override database/schema
sst extract --db PROD_DB --schema SEMANTIC
```

### Generate

```bash
# Generate all views
sst generate --all

# Generate specific views
sst generate -v view_name
sst generate -v view1 -v view2

# With defer
sst generate --all --defer-target prod

# Selective generation
sst generate --all --defer-target prod --only-modified

# Preview
sst generate --all --dry-run
```

### Deploy

```bash
# One-step deployment
sst deploy

# Deploy to specific target
sst deploy --target prod

# With defer
sst deploy --defer-target prod
sst deploy --defer-target prod --only-modified

# Skip validation
sst deploy --skip-validation
```

---

## Utility Commands

```bash
# Migrate to dbt Fusion format
sst migrate-meta models/
sst migrate-meta models/ --dry-run
sst migrate-meta models/ --backup

# Migrate with wildcard patterns
sst migrate-meta "models/analytics/shared_prefix_*"
sst migrate-meta "models/analytics/_intermediate/*"
```

---

## Common Options

| Option | Commands | Description |
|--------|----------|-------------|
| `--target`, `-t` | Most | dbt target from profiles.yml |
| `--verbose`, `-v` | Most | Detailed output |
| `--dry-run` | enrich, format, generate | Preview changes |
| `--exclude` | validate, enrich | Skip directories |

---

## Workflows

### New Project Setup

```bash
sst init
sst debug --test-connection
sst enrich --models first_model
sst validate
```

### Daily Development

```bash
sst enrich --models my_model
sst validate
sst format models/
```

### Deploy to Production

```bash
sst deploy --target prod
# Or step-by-step:
sst validate
sst extract --target prod
sst generate --target prod --all
```

### CI/CD Pipeline

```bash
# On PR:
sst validate

# On merge to main:
sst deploy --target prod
```

---

## Configuration

`sst_config.yml`:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"

validation:
  exclude_dirs: ["_intermediate"]
  strict: false
  snowflake_syntax_check: true

enrichment:
  synonym_model: 'mistral-large2'

defer:
  target: prod
```

**Note:** The dbt models directory is auto-detected from `dbt_project.yml`.

---

## Common Errors

| Error | Solution |
|-------|----------|
| manifest.json not found | Run `dbt compile` |
| Profile not found | Check `~/.dbt/profiles.yml` |
| Connection failed | Run `sst debug --test-connection` |
| Model unavailable | Use `mistral-large2` for synonyms |

---

## Links

- [Full CLI Reference](../cli/index.md)
- [Configuration Reference](config.md)
- [Getting Started](../getting-started.md)
