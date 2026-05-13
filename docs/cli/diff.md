# sst diff

Preview semantic view changes before deployment.

---

## Overview

The `diff` command compares proposed semantic view DDL (from `sst_manifest.json`) against views currently deployed in Snowflake. Like `terraform plan` for semantic views.

**Snowflake Connection:** Required (to read existing views)

---

## Quick Start

```bash
# Summary of what would change
sst diff

# Full SQL diff for changed views
sst diff --full

# Just changed view names (for scripting)
sst diff --names-only

# Diff a specific view
sst diff -v customer_360
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
| `--db` | | TEXT | From profile | Target database |
| `--schema` | `-s` | TEXT | From profile | Target schema |
| `--full` | | FLAG | False | Show complete SQL diff for changed views |
| `--names-only` | | FLAG | False | Output only changed view names |
| `--view` | `-v` | TEXT | | Diff specific view(s) (repeatable) |
| `--output` | `-o` | PATH | | Save diff to file |
| `--format` | `-f` | CHOICE | text | Output format: text, json |
| `--verbose` | | FLAG | False | Show detailed progress |

---

## Prerequisites

1. Run `sst compile` to generate `target/sst_manifest.json`
2. Snowflake credentials in `~/.dbt/profiles.yml`

---

## Output Modes

### Summary (default)

```
Semantic View Diff

  New:        1 view(s)
  Modified:   1 view(s)
  Unchanged:  1 view(s)

  New:
    + customer_lifetime_value

  Modified:
    ~ sales_analytics

  Use --full to see complete SQL diff
  To deploy: sst deploy
```

### Full (`--full`)

Shows unified diff for each modified view:

```
Modified views (1):
  ~ sales_analytics
    --- sales_analytics (deployed)
    +++ sales_analytics (proposed)
    @@ -10,3 +10,4 @@
         METRIC total_revenue AS SUM(ORDERS.ORDER_TOTAL),
    +    METRIC avg_order_value AS AVG(ORDERS.ORDER_TOTAL),
```

### Names only (`--names-only`)

```
customer_lifetime_value
sales_analytics
```

### JSON (`--format json`)

```json
{
  "summary": {
    "new": 1,
    "modified": 1,
    "unchanged": 1,
    "extra_deployed": 0
  },
  "new": ["customer_lifetime_value"],
  "modified": ["sales_analytics"],
  "unchanged": ["menu_analytics"]
}
```

---

## Workflow

```
sst compile     →  Generate manifest
sst diff        →  Preview changes
sst deploy      →  Deploy with confidence
```

---

## Troubleshooting

| Error Code | Title | Suggestion |
|-----------|-------|------------|
| SST-D001 | Connection failed | Check Snowflake credentials and network |
| SST-D002 | Manifest not found | Run `sst compile` first |
| SST-D003 | No views to compare | Define semantic views in YAML |
| SST-D004 | GET_DDL failed | Check USAGE privileges on semantic view |

---

## Related

- [sst compile](compile.md) - Compile metadata into manifest
- [sst generate](generate.md) - Deploy semantic views
- [sst deploy](deploy.md) - Full deployment workflow
