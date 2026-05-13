# sst compile

Compile SST metadata into a local manifest.

---

## Overview

The `compile` command parses all dbt model YAML files and semantic model YAML files, resolves templates, and writes a compiled manifest to `target/sst_manifest.json`. This manifest contains all metadata needed for `sst validate` and `sst generate` without a Snowflake connection.

**Snowflake Connection:** Not required

---

## Quick Start

```bash
# Compile all metadata
sst compile

# Then generate views from the manifest
sst generate --all --dry-run
```

---

## Syntax

```bash
sst compile [OPTIONS]
```

---

## Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--target` | `-t` | TEXT | Profile default | dbt target from profiles.yml |
| `--dbt` | | PATH | From dbt_project.yml | Custom dbt models path |
| `--semantic` | | PATH | From sst_config.yml | Custom semantic models path |
| `--verbose` | | FLAG | False | Show detailed output |

---

## Manifest Format

The compiled manifest (`target/sst_manifest.json`) contains:

- **metadata** — SST version, generation timestamp, dbt manifest path
- **file_checksums** — SHA-256 checksums of all tracked YAML files (used by `--only-modified`)
- **tables** — Full compiled metadata for all 11 entity types: tables, dimensions, time_dimensions, facts, metrics, relationships, relationship_columns, filters, verified_queries, custom_instructions, semantic_views

---

## How It Works

1. Loads dbt `manifest.json` for database/schema resolution
2. Discovers dbt model YAML files (from `model-paths` in `dbt_project.yml`)
3. Discovers semantic model YAML files (from `semantic_models_dir` in `sst_config.yml`)
4. Runs the two-pass parser (catalog build + template resolution)
5. Writes compiled metadata to `target/sst_manifest.json`

---

## Examples

```bash
# Basic compile
sst compile

# Compile with verbose output
sst compile --verbose

# Compile from custom paths
sst compile --dbt models/marts --semantic snowflake_semantic_models
```

### Example Output

```
Running with sst=0.3.0

Compiled in 0.4s
  Tables: 15
  Metrics: 91
  Semantic views: 3
  Files tracked: 21
  Manifest: target/sst_manifest.json
```

---

## Pipeline Integration

The compile command is the foundation of the SST pipeline:

```
sst compile  →  sst validate  →  sst generate --all
```

`sst deploy` runs this pipeline automatically. The `sst extract` command (writing SM_* tables to Snowflake) is now opt-in via `--extract-to-snowflake`.

---

## Troubleshooting

| Error Code | Issue | Solution |
|-----------|-------|---------|
| SST-C005 | Compile failed | Check YAML syntax and dbt manifest availability |
| SST-C006 | Manifest write failed | Check `target/` directory permissions |
| SST-C007 | Manifest load failed | Run `sst compile` to create the manifest |
| SST-C008 | Manifest stale | Re-run `sst compile` after modifying YAML files |
