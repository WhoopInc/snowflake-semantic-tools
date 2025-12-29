# User Guide

Learn how SST's core features work.

---

## Metadata Enrichment

### What It Does

Enrichment queries your Snowflake tables and populates dbt YAML files with:

- **Column types** - dimension, fact, or time_dimension
- **Data types** - TEXT, NUMBER, DATE, etc.
- **Sample values** - Up to 25 examples per column
- **Enum detection** - Identifies categorical columns
- **Primary key validation** - Tests provided candidates

### Manifest Auto-Detection

**Database and schema can be auto-detected from manifest.json:**

```bash
# With explicit database/schema
sst enrich models/memberships/ --database ANALYTICS --schema memberships

# With auto-detection from manifest
dbt compile --target prod  # Generates manifest.json
sst enrich models/memberships/  # Auto-detects database and schema
```

**Benefits:**
- No need to specify `--database`/`--schema` each time
- Zero drift risk (manifest is dbt's source of truth)
- Auto-handles complex routing (intermediate models, etc.)
- Works with any dbt configuration

**See:** [Getting Started Guide](getting-started.md#1-compile-your-dbt-project) for when to run `dbt compile`

### How It Works

**1. Column Classification**

| Data Type | Logic | Result |
|-----------|-------|--------|
| DATE, TIMESTAMP | Any temporal type | time_dimension |
| NUMBER | Starts with `is_` or `has_` | dimension (boolean) |
| NUMBER | Measure name (amount, count, etc.) | fact |
| TEXT, BOOLEAN | Any | dimension |

**2. Sample Values**

- Regular columns: Fetches distinct values (default: 25, configurable)
- PII columns: Empty array (security)
- Low cardinality: All values included
- High cardinality: Limited number as examples (default: 10, configurable)

Uses batched queries for performance.

To customize sample value limits, configure in `sst_config.yaml`:

```yaml
enrichment:
  distinct_limit: 25              # Number of distinct values to fetch
  sample_values_display_limit: 10 # Number to display in YAML
```

**3. Enum Detection**

- time_dimension → Always `false`
- fact → Always `false`
- dimension → `true` if fewer than `distinct_limit` unique values

**4. Enrichment Customization**

Configure sample value behavior in `sst_config.yaml`:

```yaml
enrichment:
  # Number of distinct values to fetch from Snowflake (accounts for null)
  distinct_limit: 25
  
  # Number of sample values to display in YAML files
  sample_values_display_limit: 10
```

**5. Exclusions**

Configure in `sst_config.yaml` to skip directories/paths:

```yaml
validation:
  exclude_dirs:
    - "_intermediate"                    # Simple: any dir named "_intermediate"
    - "staging"                          # Simple: any dir named "staging"  
    - "models/legacy/*"                  # Pattern: everything in specific path
    - "models/analytics/experimental/*"  # Pattern: specific subdirectory only
```

**Pattern Types:**
- **Simple name**: `"staging"` → excludes ANY directory named "staging"
- **Glob pattern**: `"models/legacy/*"` → excludes only that specific path
- **Selective**: `"models/analytics/finance/staging/*"` → excludes finance staging but NOT memberships staging

**CLI Override:**
```bash
# Temporary exclusions (adds to config for this run only)
sst validate --exclude temp,backup
sst enrich models/ --exclude experimental --database DB --schema SCHEMA
```

**Validation:**
- Run `sst validate --verbose` to see which patterns are being used
- Warns about patterns that match no files (typos/outdated config)

**5. Primary Keys**

Provide candidates via JSON file:
```json
{
  "customers": [["customer_id"]],
  "orders": [["order_id"]]
}
```

SST validates each with uniqueness queries and picks the best.

### What Gets Preserved

Enrichment NEVER overwrites:
- Existing descriptions
- Existing synonyms
- Existing primary_key
- Existing unique_keys
- Existing column_type

Enrichment ALWAYS updates:
- sample_values (fresh data)
- is_enum (current cardinality)

**Safe to run multiple times.**

**For CLI syntax:** See [CLI Reference](cli-reference.md#enrich)  
**For Python API:** See [API Reference](api-reference.md)

---

## Validation

### What It Does

Validation checks semantic models against dbt definitions:

- All table references exist
- All column references exist
- Templates resolve correctly
- No circular dependencies
- No duplicate names

**No Snowflake connection needed.**

**For complete list of all validation checks:** See [Validation Checklist](validation-checklist.md)

### What Gets Checked

**1. Table References**
- Table exists in dbt catalog
- Table has SST metadata
- Table has primary_key

**2. Column References**
- Column exists in table
- Column has correct data_type
- PII columns have no sample_values

**3. Templates**
- `{{ table('name') }}` resolves
- `{{ column('table', 'col') }}` resolves
- `{{ metric('name') }}` resolves

**4. Dependencies**
- No circular metric references
- All referenced metrics exist

**5. Duplicates**
- No duplicate metric names
- No duplicate relationship names

### Common Issues

**Table not found:**
- Check spelling
- Verify `cortex_searchable: true` in YAML

**Circular dependency:**
- Break the cycle
- Create base metrics without dependencies

**Duplicate names:**
- Rename one
- Use prefixes (finance_revenue vs sales_revenue)

**For CLI syntax:** See [CLI Reference](cli-reference.md#validate)  
**For Python API:** See [API Reference](api-reference.md)

---

## Extract and Deploy

### Understanding Environment Control

**Key concept:** The `--db` flag on `sst extract` controls WHERE metadata is deployed (environment selection).

```bash
# Development
sst extract --db SCRATCH --schema dbt_yourname

# QA
sst extract --db ANALYTICS_QA --schema SEMANTIC

# Production
sst extract --db ANALYTICS --schema SEMANTIC
```

**How manifest works with extract:**
- Manifest provides source location (where tables live: database and schema)
- `--db` flag on `extract` ONLY overrides the database in deployed metadata (defer mechanism)
- Schema always comes from manifest (not overridden by `--db` flag)
- **Result:** Same YAMLs work in all environments

**Example:**
```bash
# Model in YAML has no database/schema (uses manifest)
# Manifest says: ANALYTICS.MEMBERSHIPS
# Extract with: --db SCRATCH
# Result: Metadata written with database=SCRATCH, schema=MEMBERSHIPS
# (schema from manifest, database overridden)
```

**Note:** The `extract` command's `--schema` flag controls WHERE metadata is stored, not what schema value is written to the metadata itself. The schema value in the metadata always comes from the manifest.

**See:** [CLI Reference](cli-reference.md#extract) for full details on the extract command

---

## Best Practices

### Enrichment
1. Run after creating new models
2. Provide primary key candidates for important tables
3. Exclude unnecessary directories
4. Format YAML after enrichment

### Validation
1. Validate before committing
2. Use `--strict` during development
3. Fix errors immediately
4. Review warnings for incomplete metadata

---

## Next Steps

- **CLI Commands:** [CLI Reference](cli-reference.md)
- **Python API:** [API Reference](api-reference.md)
- **Write Models:** [Semantic Models Guide](semantic-models-guide.md)

