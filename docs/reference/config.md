# Configuration Reference

Complete reference for `sst_config.yaml` configuration options.

---

## Overview

SST is configured via `sst_config.yaml` in your dbt project root (same directory as `dbt_project.yml`). This file controls project paths, validation behavior, enrichment settings, and defer configuration.

---

## Quick Start

Minimal configuration:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
```

Full configuration with all options:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

validation:
  exclude_dirs:
    - "_intermediate"
    - "staging"
  strict: false
  snowflake_syntax_check: true

enrichment:
  distinct_limit: 25
  sample_values_display_limit: 10
  synonym_model: 'mistral-large2'
  synonym_max_count: 4

defer:
  target: prod
  state_path: ./prod_run_artifacts
  auto_compile: false
```

---

## Configuration Sections

### project

Project paths and directory configuration.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `semantic_models_dir` | string | Yes | - | Directory for semantic model definitions |
| `dbt_models_dir` | string | Yes | - | Directory for dbt models |

**Example:**

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
```

**Notes:**
- Paths are relative to project root (where `sst_config.yaml` lives)
- `semantic_models_dir` is where SST looks for metrics, relationships, filters, etc.
- `dbt_models_dir` is your standard dbt models directory

---

### validation

Validation behavior and exclusions.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `exclude_dirs` | list | No | `[]` | Directories to skip during validation |
| `strict` | boolean | No | `false` | Fail on warnings (not just errors) |
| `snowflake_syntax_check` | boolean | No | `false` | Validate SQL expressions against Snowflake |

**Example:**

```yaml
validation:
  exclude_dirs:
    - "_intermediate"
    - "staging"
    - "models/legacy/*"
  strict: false
  snowflake_syntax_check: true
```

**Exclusion Patterns:**

| Pattern | Behavior |
|---------|----------|
| `"staging"` | Excludes ANY directory named "staging" |
| `"models/legacy/*"` | Excludes only that specific path |
| `"models/*/temp/*"` | Glob pattern for specific subdirectories |

**Notes:**
- `strict: true` makes warnings block validation (useful for CI)
- `snowflake_syntax_check: true` requires Snowflake connection
- Exclusions apply to both validation and enrichment

---

### enrichment

Metadata enrichment configuration.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `distinct_limit` | integer | No | `25` | Number of distinct values to fetch from Snowflake |
| `sample_values_display_limit` | integer | No | `10` | Number of sample values to display in YAML |
| `synonym_model` | string | No | `'mistral-large2'` | Cortex LLM model for synonym generation |
| `synonym_max_count` | integer | No | `4` | Maximum synonyms per table/column |

**Example:**

```yaml
enrichment:
  distinct_limit: 25
  sample_values_display_limit: 10
  synonym_model: 'mistral-large2'
  synonym_max_count: 4
```

**LLM Models:**

| Model | Availability | Speed | Notes |
|-------|--------------|-------|-------|
| `mistral-large2` | All regions | Medium | Recommended default |
| `llama3.1-70b` | All regions | Medium | Good alternative |
| `llama3.1-8b` | All regions | Fast | Lower quality |
| `mixtral-8x7b` | All regions | Fast | Lower cost |

**Notes:**
- `distinct_limit` affects enum detection (columns with fewer values marked as enums)
- `sample_values_display_limit` controls YAML file size
- Model availability varies by region; see [Snowflake Cortex docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability)

---

### defer

Defer mode configuration for development environments.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `target` | string | No | - | Default dbt target for defer operations |
| `state_path` | string | No | - | Path to defer state artifacts (for dbt Cloud CLI) |
| `auto_compile` | boolean | No | `false` | Auto-compile manifest (dbt Core only) |

**Example:**

```yaml
defer:
  target: prod
  state_path: ./prod_run_artifacts
  auto_compile: false
```

**Notes:**
- When `target` is set, `sst generate` and `sst deploy` automatically use defer
- `state_path` is required for dbt Cloud CLI users
- `auto_compile: true` runs `dbt compile --target {defer_target}` before operations

**dbt Core workflow:**

```bash
# Compile prod manifest
dbt compile --target prod

# Generate with defer (uses config)
sst generate --all
```

**dbt Cloud CLI workflow:**

```bash
# Download manifest from dbt Cloud artifacts to state_path
# Then generate (uses config)
sst generate --all
```

---

## File Location

`sst_config.yaml` must be in your dbt project root:

```
your-dbt-project/
├── dbt_project.yml
├── sst_config.yaml          # ← Here
├── models/
├── snowflake_semantic_models/
└── target/
```

---

## Environment Variables

Configuration values can reference environment variables:

```yaml
project:
  semantic_models_dir: "{{ env_var('SST_SEMANTIC_DIR', 'snowflake_semantic_models') }}"
```

**Note:** This uses dbt's Jinja templating syntax.

---

## Validation

SST validates the configuration file on startup. Common errors:

### "Missing required field"

```yaml
# Wrong - missing required fields
validation:
  strict: true

# Correct - include project section
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
validation:
  strict: true
```

### "Directory not found"

Ensure paths are relative to project root and directories exist:

```bash
ls snowflake_semantic_models/
ls models/
```

### "Invalid model name"

```yaml
# Wrong - model doesn't exist in region
enrichment:
  synonym_model: 'gpt-4o'

# Correct - use universally available model
enrichment:
  synonym_model: 'mistral-large2'
```

---

## Examples

### Minimal Configuration

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"
```

### Development Configuration

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

validation:
  exclude_dirs:
    - "_intermediate"
  strict: false
  snowflake_syntax_check: false

enrichment:
  synonym_model: 'mistral-large2'

defer:
  target: prod
```

### Production/CI Configuration

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

validation:
  exclude_dirs:
    - "_intermediate"
    - "staging"
  strict: true
  snowflake_syntax_check: true

enrichment:
  distinct_limit: 50
  sample_values_display_limit: 15
  synonym_model: 'mistral-large2'
  synonym_max_count: 5
```

### dbt Cloud CLI Configuration

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

defer:
  target: prod
  state_path: ./prod_run_artifacts
```

---

## Related

- [Getting Started](../getting-started.md) - Initial setup
- [sst init](../cli/init.md) - Generate configuration
- [sst debug](../cli/debug.md) - Verify configuration
- [sst validate](../cli/validate.md) - Validation options
- [sst enrich](../cli/enrich.md) - Enrichment options
