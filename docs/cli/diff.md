# sst diff

Preview semantic view changes before deployment.

---

## Overview

The `diff` command compares proposed semantic view components (from `sst_manifest.json`) against views currently deployed in Snowflake. It shows only what changed: new, removed, and modified components.

**Snowflake Connection:** Required (reads deployed views via `DESCRIBE SEMANTIC VIEW`)

---

## Quick Start

```bash
# Compile first, then diff
sst compile
sst diff

# Diff a specific view
sst diff -v customer_360

# Machine-readable output
sst diff --format json
```

---

## Syntax

```bash
sst diff [OPTIONS]
```

---

## Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--target` | `-t` | TEXT | Profile default | dbt target from profiles.yml |
| `--schema` | `-s` | TEXT | From profile | Target schema |
| `--db` | | TEXT | From profile | Target database |
| `--full` | | FLAG | False | Show property-level details for modifications |
| `--names-only` | | FLAG | False | Output only changed view names (for scripting) |
| `--view` | `-v` | TEXT | All | Diff specific view(s) only |
| `--format` | `-f` | TEXT | text | Output format (`text` or `json`) |
| `--output` | `-o` | TEXT | stdout | Save diff to file |
| `--verbose` | | FLAG | False | Verbose output |

---

## Component Types

The diff compares these semantic view component types:

| Component | Key Format | Comparison |
|-----------|-----------|------------|
| TABLE | `TABLE_NAME` | Presence only |
| DIMENSION | `TABLE.NAME` | Expression |
| FACT | `TABLE.NAME` | Expression |
| METRIC | `NAME` | Base expression (window clauses excluded) |
| RELATIONSHIP | `NAME` | Reference table |
| AI_VERIFIED_QUERY | `NAME` | Question text |
| CUSTOM_INSTRUCTION | `AI_SQL_GENERATION` / `AI_QUESTION_CATEGORIZATION` | Instruction text |

---

## How It Works

1. Loads proposed components from `target/sst_manifest.json`
2. Lists deployed views via `SHOW SEMANTIC VIEWS IN db.schema`
3. For each proposed view that exists in Snowflake, runs `DESCRIBE SEMANTIC VIEW`
4. Compares components by kind and key, reporting new (`+`), removed (`-`), and modified (`~`)
5. Internal Snowflake components (e.g. `_JK_*` join keys) are filtered out

---

## Examples

```bash
# Basic diff
sst diff

# Diff with full details
sst diff --full

# Diff specific view against production
sst diff -v my_view -t prod

# Just view names for CI scripting
sst diff --names-only

# JSON output for tooling
sst diff --format json -o diff.json
```

### Example Output

```
Running with sst=0.3.0

Comparing: ANALYTICS.SEMANTIC_VIEWS

CUSTOMER_360:
  Dimensions:
    + CUSTOMERS.LOYALTY_TIER     CUSTOMERS.LOYALTY_LEVEL
  Metrics:
    - LEGACY_REVENUE
    ~ TOTAL_REVENUE              expression changed

PRODUCT_CATALOG: no changes

1 changed, 1 unchanged
```

---

## Pipeline Integration

The diff command fits into the SST workflow between compile and deploy:

```
sst compile  →  sst diff  →  sst deploy
```

Use `sst diff` to review changes before deploying, similar to `git diff` before `git commit`.

---

## Troubleshooting

| Error Code | Issue | Solution |
|-----------|-------|---------|
| SST-D001 | Connection failed | Check Snowflake credentials in `~/.dbt/profiles.yml` |
| SST-D002 | Manifest not found | Run `sst compile` first |
| SST-D003 | No views found | Ensure manifest has views and target schema exists |
| SST-D004 | Describe failed | Check view exists and you have USAGE privileges |
