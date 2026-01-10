# sst enrich

Enrich dbt YAML metadata with semantic information from Snowflake.

---

## Overview

The `enrich` command automatically populates your dbt YAML files with semantic metadata by querying your Snowflake tables. It adds column types, data types, sample values, enum detection, and optionally generates LLM-powered synonyms.

**Snowflake Connection:** Required

---

## Quick Start

```bash
# Enrich specific models by name (requires dbt manifest)
sst enrich --models customers,orders

# Enrich with all components including synonyms
sst enrich --models customers,orders --all

# Preview changes without writing
sst enrich --models customers,orders --dry-run
```

---

## Syntax

```bash
sst enrich [TARGET_PATH] [OPTIONS]
sst enrich --models MODEL_NAMES [OPTIONS]
```

**Two selection modes:**
- **By model name:** `--models name1,name2` (recommended, requires manifest)
- **By path:** `sst enrich models/directory/` or `sst enrich path/to/file.yml`

---

## Options

### Model Selection

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `TARGET_PATH` | | PATH | Path to directory, .sql file, or .yml file |
| `--models` | `-m` | TEXT | Comma-separated list of model names (requires manifest) |

**Note:** `--models` and `TARGET_PATH` are mutually exclusive.

### Component Selection

Control which metadata components to enrich:

| Option | Short | Description |
|--------|-------|-------------|
| `--column-types` | `-ct` | Enrich column types (dimension/fact/time_dimension) |
| `--data-types` | `-dt` | Enrich data types (map Snowflake types) |
| `--sample-values` | `-sv` | Enrich sample values (queries data - slower) |
| `--detect-enums` | `-de` | Detect enum columns (low cardinality) |
| `--table-synonyms` | `-ts` | Generate table-level synonyms via Cortex LLM |
| `--column-synonyms` | `-cs` | Generate column-level synonyms via Cortex LLM |
| `--synonyms` | `-syn` | Generate both table and column synonyms |
| `--all` | | Enrich ALL components including synonyms |

**Default behavior:** If no component flags are specified, defaults to `--column-types --data-types --sample-values --detect-enums`

### Force Options

Overwrite existing metadata (normally preserved):

| Option | Description |
|--------|-------------|
| `--force-synonyms` | Overwrite existing synonyms |
| `--force-column-types` | Overwrite existing column types |
| `--force-data-types` | Overwrite existing data types |
| `--force-all` | Overwrite ALL existing values |

### Connection Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--target` | `-t` | TEXT | dbt target from profiles.yml (default: profile's default) |
| `--database` | `-d` | TEXT | Override database name (default: from manifest or profile) |
| `--schema` | `-s` | TEXT | Override schema name (default: from manifest or profile) |
| `--manifest` | | PATH | Path to manifest.json (default: ./target/manifest.json) |
| `--allow-non-prod` | | FLAG | Allow enrichment from non-production manifest |

### Execution Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--exclude` | | TEXT | Comma-separated directories to exclude |
| `--dry-run` | | FLAG | Preview changes without writing files |
| `--fail-fast` | | FLAG | Stop on first error |
| `--verbose` | `-v` | FLAG | Enable verbose logging |

---

## Examples

### Basic Usage

```bash
# Enrich specific models by name (recommended)
sst enrich --models customers,orders

# Enrich a directory
sst enrich models/analytics/

# Enrich a single model by file path
sst enrich models/users/users.sql
```

### Component Selection

```bash
# Fast enrichment: only column and data types (skips expensive sample queries)
sst enrich -m customers,orders -ct -dt

# Add synonyms for natural language queries
sst enrich -m customers,orders --synonyms

# Enrich everything including synonyms
sst enrich -m customers,orders --all
```

### Force Options

```bash
# Re-generate synonyms even if they already exist
sst enrich -m customers --synonyms --force-synonyms

# Re-infer all metadata (complete refresh)
sst enrich -m customers --all --force-all
```

### Preview and Debug

```bash
# Preview changes without modifying files
sst enrich -m customers,orders --dry-run --verbose

# Exclude directories for this run
sst enrich models/ --exclude _intermediate,temp_models
```

---

## What Gets Enriched

### Column Type Classification

| Data Type | Logic | Result |
|-----------|-------|--------|
| DATE, TIMESTAMP | Any temporal type | `time_dimension` |
| NUMBER | Starts with `is_` or `has_` | `dimension` (boolean) |
| NUMBER | Measure name (amount, count, etc.) | `fact` |
| TEXT, BOOLEAN | Any | `dimension` |

### Sample Values

- Regular columns: Up to 25 distinct values (configurable)
- PII columns: Empty array (privacy protection)
- Low cardinality: All values included
- High cardinality: Limited to display limit

### Enum Detection

- `time_dimension` → Always `false`
- `fact` → Always `false`
- `dimension` → `true` if fewer than `distinct_limit` unique values

### Synonym Generation

Uses Snowflake Cortex LLMs to generate natural language synonyms:

- **Table synonyms:** Alternative names for the table
- **Column synonyms:** Alternative names for columns

---

## Configuration

Configure enrichment behavior in `sst_config.yaml`:

```yaml
enrichment:
  # Number of distinct values to fetch from Snowflake
  distinct_limit: 25
  
  # Number of sample values to display in YAML files
  sample_values_display_limit: 10
  
  # LLM model for synonym generation
  synonym_model: 'mistral-large2'  # Universally available across regions
  
  # Maximum synonyms per table/column
  synonym_max_count: 4
```

### LLM Model Availability

Available models vary by Snowflake region. `mistral-large2` is available across all regions.

Other options:
- `llama3.1-70b`, `llama3.1-8b` (Meta open models)
- `mixtral-8x7b`, `mistral-7b` (faster, lower cost)

See [Snowflake Cortex LLM documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability) for regional availability.

---

## Output

```
18:20:00  Enriching 15 model(s) in models/customers
18:20:01   1 of 15  customer_status_daily          [OK in 2.3s]
18:20:03   2 of 15  customer_lifecycle             [OK in 1.8s]
18:20:05   3 of 15  customer_summary               [OK in 2.1s]
...
18:20:30  15 of 15  customer_preferences           [OK in 1.5s]
18:20:30  Completed in 30.2s (15 enriched)
```

---

## What Gets Preserved

Enrichment **never** overwrites:
- Existing descriptions
- Existing synonyms (unless `--force-synonyms`)
- Existing primary_key
- Existing unique_keys
- Existing column_type (unless `--force-column-types`)

Enrichment **always** updates:
- sample_values (fresh data)
- is_enum (current cardinality)

**Safe to run multiple times.**

---

## Troubleshooting

### "manifest.json not found"

The `--models` flag requires a dbt manifest.

```bash
# Generate manifest first
dbt compile --target prod

# Then enrich
sst enrich --models customers
```

### "Model unavailable" during synonym generation

Cortex model not available in your region.

**Solution:** Use a universally available model:

```yaml
# sst_config.yaml
enrichment:
  synonym_model: 'mistral-large2'
```

### Slow enrichment

Sample value queries can be slow on large tables.

**Solution:** Skip sample values for faster enrichment:

```bash
sst enrich -m customers -ct -dt  # Only column and data types
```

### Columns not being enriched

Check that your dbt model YAML exists and has columns defined.

```bash
# Verify model is in manifest
sst debug --verbose
```

---

## Related

- [sst validate](validate.md) - Validate enriched metadata
- [sst format](format.md) - Format enriched YAML files
- [sst deploy](deploy.md) - Deploy to Snowflake
- [Configuration Reference](../reference/config.md) - sst_config.yaml options
- [Authentication Guide](../guides/authentication.md) - Snowflake connection setup
