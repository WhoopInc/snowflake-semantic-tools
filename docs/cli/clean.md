# sst clean

Remove SST-generated artifacts from the target directory.

---

## Overview

The `clean` command removes SST artifacts from `target/` while preserving dbt's compilation cache. Use it to reset SST state when switching branches, debugging issues, or before a fresh `sst compile`.

**Snowflake Connection:** Not required

---

## Quick Start

```bash
# Remove SST artifacts
sst clean

# Then recompile
sst compile
```

---

## Syntax

```bash
sst clean [OPTIONS]
```

---

## Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--verbose` | FLAG | False | Verbose output |

---

## What Gets Cleaned

| Artifact | Source Command | Description |
|----------|---------------|-------------|
| `target/sst_manifest.json` | `sst compile` | Compiled metadata manifest |
| `target/semantic_views/` | `sst generate --dry-run` | Generated DDL SQL files |

## What Is Preserved

| Artifact | Description |
|----------|-------------|
| `target/manifest.json` | dbt manifest (compilation cache) |
| `target/run_results.json` | dbt run results |
| `target/partial_parse.msgpack` | dbt parse cache |
| `dbt_packages/` | Installed dbt packages |

---

## Examples

```bash
# Clean SST artifacts
sst clean

# Clean then recompile and diff
sst clean && sst compile && sst diff
```

### Example Output

```
Running with sst=0.3.0

Cleaned:
  target/sst_manifest.json
  target/semantic_views/ (4 files)

Done [OK]
```

### Nothing to Clean

```
Running with sst=0.3.0

Nothing to clean

Done [OK]
```

---

## When to Use

- **After switching branches** — stale manifest from another branch can confuse `sst diff` or `sst list`
- **Before CI builds** — ensure deterministic output with no leftover state
- **When debugging** — eliminate stale artifacts as a variable
- **After renaming/deleting views** — remove orphaned DDL files from `target/semantic_views/`

---

## sst clean vs dbt clean

| Command | Removes SST artifacts | Removes dbt cache | Removes dbt_packages |
|---------|-----------------------|--------------------|-----------------------|
| `sst clean` | Yes | No | No |
| `dbt clean` | Yes (removes entire `target/`) | Yes | Yes |

Use `sst clean` for a lightweight reset. Use `dbt clean` for a full project reset.

---

## Troubleshooting

| Error Code | Issue | Solution |
|-----------|-------|---------|
| SST-K001 | Target directory not found | Nothing to clean — directory doesn't exist yet |
| SST-K002 | Could not remove artifact | Check file permissions or close editors locking the files |
