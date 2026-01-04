# dbt Fusion Meta Migration Guide

This guide explains how to migrate your dbt YAML files from the legacy `meta.sst` format to the new `config.meta.sst` format required by dbt Fusion.

## Why This Migration is Needed

dbt Fusion (dbt's next-generation Rust engine) requires that all `meta` configurations be placed under `config:` instead of at the top level. This is a **hard requirement** in dbt Fusion and will cause compilation errors if not addressed.

### Timeline

| dbt Version | Behavior |
|-------------|----------|
| dbt Core 1.9 and earlier | Legacy `meta.sst` works fine |
| dbt Core 1.10+ | Deprecation warning for legacy format |
| dbt Fusion | **ERROR** - compilation fails with legacy format |

### Critical Path

**Column-level `meta`** is especially critical - it **MUST** be migrated for dbt Fusion compatibility. Model-level `meta` is less urgent but recommended.

## Format Comparison

### Legacy Format (Deprecated)

```yaml
version: 2

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

### New Format (dbt Fusion Compatible)

```yaml
version: 2

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

## Migration Options

### Option 1: Automatic Migration (Recommended)

Use the `sst migrate-meta` command to automatically migrate all YAML files:

```bash
# Preview changes without modifying files
sst migrate-meta models/ --dry-run

# Migrate all YAML files in a directory
sst migrate-meta models/

# Migrate with backups
sst migrate-meta models/ --backup

# Migrate a single file
sst migrate-meta models/analytics/users/users.yml
```

### Option 2: Manual Migration

If you prefer to migrate manually:

1. **Find** all occurrences of `meta.sst` in your YAML files
2. **Move** the `sst` block from `meta:` to `config: meta:`
3. **Remove** the old `meta: sst:` block

### Option 3: Re-run Enrichment

Running `sst enrich` will automatically write in the new format. However, this may re-detect metadata values. Use the `--force-*` flags to control which fields are updated.

## Detecting Legacy Format

SST will emit deprecation warnings when it detects legacy format during operations:

```
[DEPRECATED-WARNING] Model 'orders' uses meta.sst pattern. This will be an ERROR in dbt Fusion. Migrate to config.meta.sst pattern. Run 'sst migrate-meta' to auto-fix.

[DEPRECATED-CRITICAL] Column 'orders.user_id' uses meta.sst pattern. This will be an ERROR in dbt Fusion. Migrate to config.meta.sst pattern. Run 'sst migrate-meta' to auto-fix.
```

- **WARNING**: Model-level legacy format (less urgent)
- **CRITICAL**: Column-level legacy format (must fix for dbt Fusion)

## Backward Compatibility

SST maintains backward compatibility during the transition:

| Operation | Legacy Format | New Format |
|-----------|---------------|------------|
| Reading metadata | ✅ Supported (with warning) | ✅ Supported |
| Writing metadata | ❌ Never written | ✅ Always written |
| dbt Core 1.10 | ⚠️ Deprecation warning | ✅ Works |
| dbt Fusion | ❌ Error | ✅ Works |

## Verifying Migration

After migration, verify your files:

```bash
# Check for any remaining legacy format
grep -r "  meta:" --include="*.yml" models/ | grep -v "config:"

# Or validate with SST (should have no deprecation warnings)
sst validate models/
```

## Troubleshooting

### Migration Command Shows No Changes

If `sst migrate-meta` reports "No files need migration", your files are already in the new format.

### Validation Errors After Migration

The migration only changes the location of SST metadata, not its content. If you see validation errors after migration, they were likely pre-existing. Run `sst validate` to identify and fix them.

### Conflicting Metadata

If both `meta.sst` and `config.meta.sst` exist in a file:
- SST reads from `config.meta.sst` (new format takes priority)
- Migration preserves existing `config.meta.sst` values
- Legacy values only migrate if the key doesn't exist in new location

## Related Documentation

- [Getting Started Guide](getting-started.md)
- [User Guide](user-guide.md)
- [CLI Reference](cli-reference.md)

