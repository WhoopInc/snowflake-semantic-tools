# sst migrate-meta

Migrate meta.sst to config.meta.sst (dbt Fusion compatibility).

---

## Overview

The `migrate-meta` command migrates dbt YAML files from the legacy `meta.sst` format to the new `config.meta.sst` format required by dbt Fusion. This is a one-time migration needed when upgrading to dbt Fusion or preparing for the transition.

**Snowflake Connection:** Not required

---

## Quick Start

```bash
# Preview migration for a directory
sst migrate-meta models/ --dry-run

# Migrate all YAML files in a directory
sst migrate-meta models/

# Migrate with backups
sst migrate-meta models/ --backup
```

---

## Syntax

```bash
sst migrate-meta PATH [OPTIONS]
```

---

## Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `PATH` | | PATH | Required | File or directory to migrate |
| `--dry-run` | | FLAG | False | Preview changes without modifying files |
| `--backup` | | FLAG | False | Create .bak backup files before modifying |
| `--verbose` | `-v` | FLAG | False | Show detailed migration notes |

---

## Why This Migration is Needed

dbt Fusion (dbt's next-generation Rust engine) requires that all `meta` configurations be placed under `config:` instead of at the top level.

| dbt Version | Behavior |
|-------------|----------|
| dbt Core 1.9 and earlier | Legacy `meta.sst` works fine |
| dbt Core 1.10+ | Deprecation warning for legacy format |
| dbt Fusion | **ERROR** - compilation fails |

**Column-level `meta`** is especially critical - it **must** be migrated for dbt Fusion compatibility.

---

## Format Comparison

### Before (Legacy)

```yaml
models:
  - name: orders
    description: "Customer orders"
    meta:
      sst:
        cortex_searchable: true
        primary_key: id
    columns:
      - name: id
        description: "Order ID"
        meta:
          sst:
            column_type: dimension
            data_type: text
```

### After (dbt Fusion Compatible)

```yaml
models:
  - name: orders
    description: "Customer orders"
    config:
      meta:
        sst:
          cortex_searchable: true
          primary_key: id
    columns:
      - name: id
        description: "Order ID"
        config:
          meta:
            sst:
              column_type: dimension
              data_type: text
```

---

## Examples

```bash
# Preview migration (recommended first step)
sst migrate-meta models/ --dry-run

# Migrate all YAML files in a directory
sst migrate-meta models/

# Migrate with backups (creates .yml.bak files)
sst migrate-meta models/ --backup

# Migrate a single file
sst migrate-meta models/analytics/users/users.yml

# Verbose output showing each field migrated
sst migrate-meta models/ --verbose
```

---

## Output

```
Running with sst=0.2.2

Migrating 15 YAML file(s)...

  [MIGRATED] models/orders/orders.yml (1 model(s), 5 column(s))
  [MIGRATED] models/users/users.yml (1 model(s), 8 column(s))
  
[SUCCESS] Migrated 2 file(s): 2 model(s), 13 column(s)
```

---

## Best Practices

### 1. Run with --dry-run First

Always preview changes before modifying files:

```bash
sst migrate-meta models/ --dry-run
```

### 2. Create Backups for Important Files

Use `--backup` flag for safety:

```bash
sst migrate-meta models/ --backup
```

### 3. Migrate Before Upgrading to dbt Fusion

Column-level meta **must** be migrated before using dbt Fusion.

### 4. Verify After Migration

Run validation to ensure no issues:

```bash
sst validate
```

---

## Detecting Legacy Format

SST emits deprecation warnings when it detects legacy format:

```
[DEPRECATED-WARNING] Model 'orders' uses meta.sst pattern. 
This will be an ERROR in dbt Fusion. Migrate to config.meta.sst pattern. 
Run 'sst migrate-meta' to auto-fix.

[DEPRECATED-CRITICAL] Column 'orders.user_id' uses meta.sst pattern. 
This will be an ERROR in dbt Fusion. Migrate to config.meta.sst pattern. 
Run 'sst migrate-meta' to auto-fix.
```

- **WARNING:** Model-level legacy format (less urgent)
- **CRITICAL:** Column-level legacy format (must fix)

---

## Backward Compatibility

SST maintains backward compatibility during the transition:

| Operation | Legacy Format | New Format |
|-----------|---------------|------------|
| Reading metadata | Supported (with warning) | Supported |
| Writing metadata | Never written | Always written |
| dbt Core 1.10 | Deprecation warning | Works |
| dbt Fusion | Error | Works |

---

## Troubleshooting

### "No files need migration"

Your files are already in the new format. Verify with:

```bash
grep -r "  meta:" --include="*.yml" models/ | grep -v "config:"
```

### Validation errors after migration

The migration only changes metadata location, not content. Pre-existing errors will remain:

```bash
sst validate --verbose
```

### Conflicting metadata

If both `meta.sst` and `config.meta.sst` exist:

- SST reads from `config.meta.sst` (new format takes priority)
- Migration preserves existing `config.meta.sst` values
- Legacy values only migrate if key doesn't exist in new location

---

## Related

- [dbt Fusion Migration Guide](../guides/dbt-fusion-migration.md) - Complete migration guide
- [sst format](format.md) - Format YAML after migration
- [sst validate](validate.md) - Validate after migration
