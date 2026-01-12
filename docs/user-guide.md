# User Guide

Learn how SST's core features work.

---

## Metadata Enrichment

### What It Does

Enrichment queries your Snowflake tables and populates dbt YAML files with:

- **Column types** - dimension, fact, or time_dimension
- **Data types** - TEXT, NUMBER, DATE, etc.
- **Sample values** - Example values per column (default: 25, configurable via `distinct_limit`)
- **Enum detection** - Identifies categorical columns

### Manifest Auto-Detection

**Database and schema are auto-detected from manifest.json:**

```bash
# Compile to generate manifest.json
dbt compile --target prod

# Enrich specific models (database/schema from manifest)
sst enrich --models customers,orders

# Or enrich all models in a directory
sst enrich models/marts/
```

**Benefits:**
- No need to specify `--database`/`--schema` manually
- Zero drift risk (manifest is dbt's source of truth)
- Auto-handles complex routing (intermediate models, etc.)
- Works with any dbt configuration

**See:** [Getting Started Guide](getting-started.md) for the full workflow

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

To customize sample value limits, configure in `sst_config.yml`:

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

Configure sample value behavior in `sst_config.yml`:

```yaml
enrichment:
  # Number of distinct values to fetch from Snowflake (accounts for null)
  distinct_limit: 25
  
  # Number of sample values to display in YAML files
  sample_values_display_limit: 10
```

**5. Exclusions**

Configure in `sst_config.yml` to skip directories/paths:

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
sst enrich --models customers --exclude experimental
```

**Validation:**
- Run `sst validate --verbose` to see which patterns are being used
- Warns about patterns that match no files (typos/outdated config)

### What Gets Preserved

By default, enrichment preserves existing values:
- Existing descriptions (including empty strings and null values)
- Existing synonyms (use `--force-synonyms` to overwrite)
- Existing primary_key
- Existing unique_keys
- Existing column_type (use `--force-column-types` to overwrite)
- Existing data_type (use `--force-data-types` to overwrite)

**Tip:** Use `--force-all` to overwrite everything and refresh all metadata.

Enrichment ALWAYS updates (cannot be preserved):
- sample_values (fresh data from Snowflake)
- is_enum (current cardinality)

Enrichment ADDS if missing:
- `description: ''` placeholder (model and column level) for new items only

**Safe to run multiple times.**

**For CLI syntax:** See [sst enrich](cli/enrich.md)  
**For Python API:** See [API Reference](reference/api.md)

---

## Validation

### What It Does

Validation checks semantic models against dbt definitions:

- All table references exist
- All column references exist
- Templates resolve correctly
- No circular dependencies
- No duplicate names
- SQL syntax validation (optional, requires Snowflake connection)

**Basic validation requires no Snowflake connection.** Optional Snowflake syntax validation can be enabled in `sst_config.yml`:

```yaml
validation:
  snowflake_syntax_check: true  # Validates SQL expressions against Snowflake
```

**For complete list of all validation checks:** See [Validation Rules](concepts/validation-rules.md)

### What Gets Checked

**1. Table References**
- Table exists in dbt catalog
- Table has SST metadata
- Table has primary_key defined

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
- Names are normalized for comparison (e.g., `Total_Revenue` and `total_revenue` are duplicates)

**6. SQL Syntax (optional)**
- Metric expressions are valid Snowflake SQL
- Filter expressions are valid Snowflake SQL
- Verified queries execute successfully
- Catches typos in function names with "Did you mean?" suggestions

### Common Issues

**Table not found:**
- Check spelling in your template reference
- Ensure the dbt model exists and has been compiled
- Look for "Did you mean?" suggestions in validation output

**Circular dependency:**
- Break the cycle
- Create base metrics without dependencies

**Duplicate names:**
- Rename one
- Use prefixes (finance_revenue vs sales_revenue)
- Note: Names like `Total_Revenue`, `total_revenue`, and `TotalRevenue` are considered duplicates

**For CLI syntax:** See [sst validate](cli/validate.md)  
**For Python API:** See [API Reference](reference/api.md)

---

## Extract and Deploy

### Understanding Environment Control

SST uses **dbt targets** to control which environment you're working with. This leverages your existing dbt profile configuration.

```bash
# Development (uses dev target from profiles.yml)
sst extract --target dev
sst generate --target dev
sst deploy --target dev

# Production
sst extract --target prod
sst generate --target prod
sst deploy --target prod
```

**How it works:**
- Database and schema come from your dbt profile's target configuration
- Same semantic model YAMLs work in all environments
- No need to specify `--database` or `--schema` manually

**Example profiles.yml:**
```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: snowflake
      database: DEV
      schema: DBT_YOURNAME
      # ... other settings
    prod:
      type: snowflake
      database: ANALYTICS
      schema: SEMANTIC
      # ... other settings
```

**See:** [sst extract](cli/extract.md) for full details

---

## Best Practices

### Enrichment
1. Run after creating new models
2. Manually specify `primary_key` in your model YAML
3. Exclude unnecessary directories
4. Format YAML after enrichment

### Validation
1. Validate before committing
2. Use `--strict` during development
3. Fix errors immediately
4. Review warnings for incomplete metadata

---

## Next Steps

- **CLI Commands:** [CLI Reference](cli/index.md)
- **Python API:** [API Reference](reference/api.md)
- **Write Models:** [Semantic Models Guide](concepts/semantic-models.md)

